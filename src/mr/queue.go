package mr

import "time"

// 超时阻塞队列
type TimeoutQueue struct {
	queue chan interface{}
	waitTime time.Duration
}

func NewTaskQueue(maxQueueLen int, waitTime time.Duration) (tq *TimeoutQueue){
	tq = &TimeoutQueue{}
	tq.queue = make(chan interface{}, maxQueueLen)
	tq.waitTime = waitTime
	return tq
}

// 超时阻塞Push
func (tq *TimeoutQueue) Push(data interface{}) bool {
	t := time.After(tq.waitTime)
	select {
	case tq.queue <- data:
		return true
	case <-t:
		return false
	}
}

// 超时阻塞Pop
func (tq *TimeoutQueue) Pop() (data interface{}, ok bool) {
	t := time.After(tq.waitTime)
	select {
	case data = <- tq.queue:
		return data, true
	case <-t:
		return nil, false
	}
}

func (tq *TimeoutQueue) Size() int {
	return len(tq.queue)
}
