package kvraft

const (
	OK                    = "OK"
	ErrNoKey              = "ErrNoKey"
	ErrWrongLeader        = "ErrWrongLeader"
	ErrClosed             = "ErrClosed"
	ErrReAppearingIndices = "ErrReAppearingIndices"
)

type Err string
type OpType int

//// Put or Append
//type PutAppendArgs struct {
//	Key   string
//	Value string
//	Op    string // "Put" or "Append"
//	// You'll have to add definitions here.
//	// Field names must start with capital letters,
//	// otherwise RPC will break.
//}
//
//type PutAppendReply struct {
//	Err Err
//}
//
//type GetArgs struct {
//	Key string
//	// You'll have to add definitions here.
//}
//
//type GetReply struct {
//	Err   Err
//	Value string
//}

const (
	OpTypeGet OpType = iota
	OpTypePut
	OpTypeAppend
)

type OpReq struct {
	Type     OpType
	ClientID int64
	SeqID    int64 // 每次操作都递增的ID
	Key      string
	Value    string
}

type OpResp struct {
	Type     OpType
	ClientID int64
	SeqID    int64 // 每次操作都递增的ID
	Err      Err
	Value    string
}
