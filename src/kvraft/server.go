package kvraft

import (
	"LanjunC/mit6.824/labgob"
	"LanjunC/mit6.824/labrpc"
	"LanjunC/mit6.824/raft"
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"time"
)

const Debug = 1

func (kv *KVServer) DPrint(format string, a ...interface{}) (n int, err error) {
	if Debug <= 0 {
		return
	}
	s := fmt.Sprintf(format, a...)
	fmt.Println(fmt.Sprintf("[%v] | %s", kv.me, s))
	return
}

//type Op struct {
//	// Your definitions here.
//	// Field names must start with capital letters,
//	// otherwise RPC will break.
//}

type serverContext struct {
	req    *OpReq
	respCh chan *OpResp
	term   int64
	index  int64
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	//dead    int32 // set by Kill()
	closeCh      chan struct{}
	maxraftstate int64 // snapshot if log grows this big
	getSnapCh    chan raft.GetSnapReq

	// Your definitions here.
	store         map[string]string
	applyIndex    int64
	idx2Context   map[int]*serverContext // log index -> context
	id2LatestResp map[int64]OpResp       // clientId -> latest resp
}

//func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
//	// Your code here.
//}
//
//func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//}

func (kv *KVServer) Request(req *OpReq, resp *OpResp) {
	//if kv.killed() {
	//	*resp = OpResp{
	//		Type:     req.Type,
	//		ClientID: req.ClientID,
	//		SeqID:    req.SeqID,
	//		Err:      ErrClosed,
	//		Value:    "",
	//	}
	//	kv.DPrint("Request: ErrClosed.")
	//	return
	//}
	kv.mu.Lock()
	//defer kv.mu.Unlock() // 大锁会导致the four-way deadlock, 因此start前必须释放锁
	kv.DPrint("Request: get req=%+v", req)
	if kv.preProcess(req, resp) {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(*req)
	if !isLeader {
		*resp = OpResp{
			Type:     req.Type,
			ClientID: req.ClientID,
			SeqID:    req.SeqID,
			Err:      ErrWrongLeader,
			Value:    "",
		}
		kv.DPrint("Request: ErrWrongLeader.")
		return
	}
	// 再次preProcess， 因为前面释放锁了
	kv.mu.Lock()
	if kv.preProcess(req, resp) {
		kv.mu.Unlock()
		return
	}
	respCh := make(chan *OpResp)
	serverCtx := &serverContext{
		req:    req,
		respCh: respCh,
		term:   int64(term),
		index:  int64(index),
	}
	// avoid [bad case]: Re-appearing indices
	if v, ok := kv.idx2Context[index]; ok {
		kv.DPrint("find re-appearing indices, try to fix it. Index=%v, old context=%+v", index, *v)
		// find Re-appearing indices, r
		v.respCh <- &OpResp{
			Type:     v.req.Type,
			Value:    "",
			Err:      ErrTimeout,
			ClientID: v.req.ClientID,
			SeqID:    v.req.SeqID,
		}
	}
	kv.DPrint("Request:login watcher, index=%v, serverCtx=%+v", index, *serverCtx)
	kv.idx2Context[index] = serverCtx
	kv.mu.Unlock()
	*resp = *<-respCh
	// [bad case]Don`t place unlock here(must before respCh channel), or onApply can`t get lock!
	// can find the holding-lock timeout by go-deadlock
	//kv.mu.Unlock()
	return
}

// check是否是已经被处理的请求
// true则直接返回上一次的处理结果
func (kv *KVServer) preProcess(req *OpReq, resp *OpResp) bool {
	if kv.killed() {
		*resp = OpResp{
			Type:     req.Type,
			ClientID: req.ClientID,
			SeqID:    req.SeqID,
			Err:      ErrClosed,
			Value:    "",
		}
		kv.DPrint("preProcess: ErrClosed.")
		return true
	}
	latestResp, exist := kv.id2LatestResp[req.ClientID]
	if exist {
		if latestResp.ClientID != req.ClientID {
			panic(fmt.Sprintf("invalid clientID: latestResp.ClientID=%v != req.ClientID=%v",
				latestResp.ClientID, req.ClientID))
		}
		if latestResp.SeqID > req.SeqID {
			panic(fmt.Sprintf("latestResp.SeqID=%v > req.SeqID=%v", latestResp.SeqID, req.SeqID))
		}
		if latestResp.SeqID == req.SeqID {
			*resp = latestResp
			kv.DPrint("preProcess: latestResp.SeqID == req.SeqID=%v", req.SeqID)
			return true
		}
	}
	return false

}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	// Your code here, if desired.
	close(kv.closeCh)
}

func (kv *KVServer) killed() bool {
	select {
	case _, ok := <-kv.closeCh:
		return !ok
	default:
		return false
	}
}

func (kv *KVServer) eventLoop() {
	for {
		select {
		case <-kv.closeCh:
			kv.onClose()
			return
		case apply := <-kv.applyCh:
			kv.onApply(apply)
		case getSnapReq := <-kv.getSnapCh:
			respCh := getSnapReq.RespCh
			respCh <- *kv.onGetSnap()
		}
	}
}

func (kv *KVServer) onClose() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for index, ctx := range kv.idx2Context {
		ctx.respCh <- &OpResp{
			Type:     ctx.req.Type,
			ClientID: ctx.req.ClientID,
			SeqID:    ctx.req.SeqID,
			Err:      ErrClosed,
			Value:    "",
		}
		kv.DPrint("delete watcher, index=%v", index)
		delete(kv.idx2Context, index)
	}
}

func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if applyMsg.CommandValid {
		kv.DPrint("onApply: has agreement on applyMsg=%+v", applyMsg)
		if int64(applyMsg.CommandIndex) <= kv.applyIndex {
			panic(fmt.Sprintf("applyMsg.CommandIndex=%v <= kv.applyIndex=%v", applyMsg.CommandIndex, kv.applyIndex))
			// todo 崩溃的raft回来后appliedIndex和commitIdex都为0... check??
		}
		req, ok := applyMsg.Command.(OpReq)
		if !ok {
			panic("command mismatches OpReq type")
		}
		ctx, existCtx := kv.idx2Context[applyMsg.CommandIndex]
		if existCtx {
			kv.DPrint("onApply: exists context=%+v", ctx)
			// same index && different term, leader has changed after crash
			if ctx.term != applyMsg.CommandTerm {
				ctx.respCh <- &OpResp{
					Type:     ctx.req.Type,
					ClientID: ctx.req.ClientID,
					SeqID:    ctx.req.SeqID,
					Err:      ErrWrongLeader,
					Value:    "",
				}
				kv.DPrint("delete watcher^^, index=%v", applyMsg.CommandIndex)
				delete(kv.idx2Context, applyMsg.CommandIndex)
				// continue with id2LatestResp
				existCtx = false
				ctx = nil
			}
		} else {
			// apply on follower
			kv.DPrint("onApply: exists no context with index=%v", applyMsg.CommandIndex)
		}
		latestResp, ok := kv.id2LatestResp[req.ClientID]
		if ok {
			kv.DPrint("onApply: exists latestResp=%+v", latestResp)
			if latestResp.ClientID != req.ClientID {
				panic(fmt.Sprintf("invalid clientID: latestResp.ClientID=%v != req.ClientID=%v",
					latestResp.ClientID, req.ClientID))
			}
			if latestResp.SeqID > req.SeqID {
				panic(fmt.Sprintf("latestResp.SeqID=%v > req.SeqID=%v", latestResp.SeqID, req.SeqID))
			}
			if latestResp.SeqID == req.SeqID {
				if existCtx {
					ctx.respCh <- &latestResp
					kv.DPrint("delete watcher**, index=%v", applyMsg.CommandIndex)
					delete(kv.idx2Context, applyMsg.CommandIndex)
				}
				return
			}
		}
		resp := &OpResp{
			Type:     req.Type,
			ClientID: req.ClientID,
			SeqID:    req.SeqID,
			Err:      OK,
			Value:    "",
		}
		switch resp.Type {
		case OpTypeGet:
			v, exist := kv.store[req.Key]
			if exist {
				resp.Value = v
			} else {
				resp.Err = ErrNoKey
			}
		case OpTypePut:
			kv.store[req.Key] = req.Value
		case OpTypeAppend:
			kv.store[req.Key] = kv.store[req.Key] + req.Value
		}

		kv.DPrint("onApply: final resp=%+v, update latest resp", resp)
		kv.id2LatestResp[req.ClientID] = *resp
		kv.applyIndex = int64(applyMsg.CommandIndex)
		if existCtx {
			ctx.respCh <- resp
			kv.DPrint("delete watcher$$, index=%v", applyMsg.CommandIndex)
			delete(kv.idx2Context, applyMsg.CommandIndex)
		}
	} else {
		// applyMsg about snapshot
		kv.DPrint("receive applyMsg about snapshot, applyMsg=%+v", applyMsg)
		if kv.applyIndex > int64(applyMsg.CommandIndex) {
			panic(fmt.Sprintf("can`t install snapshot while kv.applyIndex=%v > applyMsg.CommandIndex=%v", kv.applyIndex, applyMsg.CommandIndex))
		}
		kv.applyIndex = int64(applyMsg.CommandIndex)
		bytes, ok := applyMsg.Command.([]byte)
		if !ok {
			panic("command type mismatched")
		}
		kv.installSnapshot(bytes)
		// 清除过期context
		for idx, ctx := range kv.idx2Context {
			if idx <= applyMsg.CommandIndex {
				kv.DPrint("find a context=%+v out of date in index=%v", ctx, idx)
				ctx.respCh <- &OpResp{
					Type:     ctx.req.Type,
					ClientID: ctx.req.ClientID,
					SeqID:    ctx.req.SeqID,
					Err:      ErrTimeout,
					Value:    "",
				}
				kv.DPrint("delete watcher>>, index=%v", idx)
				delete(kv.idx2Context, idx)
			}
		}
	}
}

func (kv *KVServer) onGetSnap() *raft.GetSnapResp {
	resp := &raft.GetSnapResp{
		Index: kv.applyIndex,
		Data:  nil,
	}
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.id2LatestResp)
	e.Encode(kv.store)
	resp.Data = w.Bytes()
	return resp
}

func (kv *KVServer) installSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		panic("data is empty while installing snapshot")
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.id2LatestResp) != nil || d.Decode(&kv.store) != nil {
		panic("decode failed while installing snapshot")
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(OpReq{}) // todo

	// You may need initialization code here.
	applyCh := make(chan raft.ApplyMsg)

	// DEBUG
	deadlock.Opts.DeadlockTimeout = time.Second // 获取锁超时控制
	//deadlock.Opts.Disable = true

	kv := &KVServer{
		mu:            deadlock.Mutex{},
		me:            me,
		rf:            nil, // set later
		applyCh:       applyCh,
		closeCh:       make(chan struct{}),
		maxraftstate:  int64(maxraftstate),
		getSnapCh:     nil, // set later
		store:         make(map[string]string),
		applyIndex:    0,
		idx2Context:   make(map[int]*serverContext),
		id2LatestResp: make(map[int64]OpResp),
	}
	if maxraftstate > 0 {
		kv.getSnapCh = make(chan raft.GetSnapReq)
	}
	kv.rf = raft.Make(servers, me, persister, applyCh, int64(maxraftstate), kv.getSnapCh)

	// You may need initialization code here.
	go kv.eventLoop()
	return kv
}
