package ingestor

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

var globalReservationQueue = NewReservationQueue()

type reservation struct {
	copySender <-chan copyRequest
	index      int

	lock      sync.Mutex
	startTime time.Time
}

func newReservation(cs <-chan copyRequest, startTime time.Time) *reservation {
	return &reservation{cs, -1, sync.Mutex{}, startTime}
}

func (res *reservation) Update(rq *ReservationQueue, t time.Time) {
	rest := res.GetStartTime()

	if t.Before(rest) {
		//this should happen rarely
		res.SetStartTime(t)
		rq.update(res)
	}
}

func (res *reservation) GetStartTime() time.Time {
	res.lock.Lock()
	defer res.lock.Unlock()
	return res.startTime
}

func (res *reservation) SetStartTime(t time.Time) {
	res.lock.Lock()
	defer res.lock.Unlock()

	//double check that it's before
	if t.Before(res.startTime) {
		res.startTime = t
	}
}

// reservationQueueInternal implements heap.Interface
type reservationQueueInternal []*reservation

func newReservationQueueInternal() *reservationQueueInternal {
	q := make(reservationQueueInternal, 0, 100)
	return &q
}

func (res reservationQueueInternal) Len() int { return len(res) }

func (res reservationQueueInternal) Less(i, j int) bool {
	return res[i].GetStartTime().Before(res[j].GetStartTime())
}

func (res reservationQueueInternal) Swap(i, j int) {
	res[i], res[j] = res[j], res[i]
	res[i].index = i
	res[j].index = j
}

func (res *reservationQueueInternal) Push(x interface{}) {
	n := len(*res)
	item := x.(*reservation)
	item.index = n
	*res = append(*res, item)
}

func (res *reservationQueueInternal) Pop() interface{} {
	old := *res
	n := len(old)
	item := old[n-1]
	item.index = -1 //for safety
	old[n-1] = nil  // avoid memory leak
	*res = old[0 : n-1]
	return item
}

type Reservation interface {
	Update(*ReservationQueue, time.Time)
}

type ReservationQueue struct {
	lock sync.Mutex
	cond sync.Cond
	q    *reservationQueueInternal
}

func NewReservationQueue() *ReservationQueue {
	res := &ReservationQueue{lock: sync.Mutex{}, q: newReservationQueueInternal()}
	res.cond = *sync.NewCond(&res.lock)
	return res
}

func (rq *ReservationQueue) Add(cs <-chan copyRequest, startTime time.Time) Reservation {
	si := newReservation(cs, startTime)

	rq.lock.Lock()
	defer rq.lock.Unlock()

	if rq.q.Len() == 0 {
		rq.cond.Broadcast()
	}

	heap.Push(rq.q, si)
	return si
}

// Peek gives the first startTime (blocking)
func (rq *ReservationQueue) Peek() time.Time {
	rq.lock.Lock()
	defer rq.lock.Unlock()
	for rq.q.Len() == 0 {
		rq.cond.Wait()
	}

	first := (*rq.q)[0]
	return first.GetStartTime()
}

// Pop returns the first copyRequest channel (blocking until context is expired).
// can return nil if context expires
func (rq *ReservationQueue) Pop(ctx context.Context) <-chan copyRequest {
	rq.lock.Lock()
	defer rq.lock.Unlock()

	if rq.q.Len() > 0 {
		res := heap.Pop(rq.q).(*reservation)
		return res.copySender
	} else if ctx.Err() == nil {
		go func() {
			<-ctx.Done()
			rq.cond.Broadcast()
		}()
		for rq.q.Len() == 0 && ctx.Err() == nil {
			rq.cond.Wait()
		}
		if ctx.Err() != nil {
			return nil
		}
		res := heap.Pop(rq.q).(*reservation)
		return res.copySender
	}
	return nil
}

func (rq *ReservationQueue) update(res *reservation) {
	rq.lock.Lock()
	defer rq.lock.Unlock()
	if res.index < 0 { //has already been poped
		return
	}
	heap.Fix(rq.q, res.index)
}
