package kvraft

import (
	"LanjunC/mit6.824/labrpc"
	"fmt"
	"time"
)
import "crypto/rand"
import "math/big"

const (
	opTimeout = 300 * time.Millisecond // 每次操作超时事件
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID     int64
	curSeqID     int64
	lastLeaderID int
	opCtxCh      chan ckContext
}

type ckContext struct {
	req    *OpReq
	respCh chan *OpResp
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	// You'll have to add code here.
	ck := &Clerk{
		servers:      servers,
		clientID:     nrand(),
		curSeqID:     0,
		lastLeaderID: 0, // get later
		opCtxCh:      make(chan ckContext),
	}

	go ck.eventLoop()
	return ck
}

func (ck *Clerk) eventLoop() {
	for opCtx := range ck.opCtxCh {
		opCtx.req.SeqID = ck.curSeqID // 事件的请求和eventLoop开始处理的顺序不一定一致，开始处理请求时才设置
	LOOP:
		for {
			timer := time.NewTimer(opTimeout)
			resp := &OpResp{}
			callRetCh := make(chan bool)
			go func() {
				callRetCh <- ck.servers[ck.lastLeaderID].Call("KVServer.Request", opCtx.req, resp)
			}()
			select {
			case <-timer.C:
				ck.changeLeaderID("timeout")
			case ok := <-callRetCh:
				// disconnet with cur peer or lost reply
				if !ok {
					ck.changeLeaderID("call failed")
					continue
				}
				switch resp.Err {
				case OK:
					fallthrough
				case ErrNoKey:
					opCtx.respCh <- resp
					ck.curSeqID++
					break LOOP
				case ErrWrongLeader:
					fallthrough
				case ErrClosed:
					fallthrough
				case ErrTimeout:
					ck.changeLeaderID(string(resp.Err))
				default:
					panic(fmt.Sprintf("invalid resp err=%s", resp.Err))
				}
			}
		}

	}
}

func (ck *Clerk) changeLeaderID(reason string) {
	//fmt.Printf("change old leader=%v, because of %v\n", ck.lastLeaderID, reason)
	ck.lastLeaderID = (ck.lastLeaderID + 1) % len(ck.servers)
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	respCh := make(chan *OpResp)
	ck.opCtxCh <- ckContext{
		req: &OpReq{
			Type:     OpTypeGet,
			ClientID: ck.clientID,
			SeqID:    0, // set later in eventLoop
			Key:      key,
			Value:    "",
		},
		respCh: respCh,
	}
	resp := <-respCh
	switch resp.Err {
	case OK:
		return resp.Value
	case ErrNoKey:
		return ""
	default:
		panic(fmt.Sprintf("invalid err[%v] when [Get]", resp.Err))
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	reqType := OpTypePut
	if op == "Append" {
		reqType = OpTypeAppend
	}
	respCh := make(chan *OpResp)
	ck.opCtxCh <- ckContext{
		req: &OpReq{
			Type:     reqType,
			ClientID: ck.clientID,
			SeqID:    0, // set later in eventLoop
			Key:      key,
			Value:    value,
		},
		respCh: respCh,
	}
	resp := <-respCh
	switch resp.Err {
	case OK:
		return
	default:
		panic(fmt.Sprintf("invalid err[%v] when [%v]", resp.Err, op))
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
