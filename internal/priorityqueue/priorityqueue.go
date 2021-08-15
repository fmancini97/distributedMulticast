package priorityqueue

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Item struct {
	Value    interface{} // The value of the item; arbitrary.
	Priority interface{} // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type PriorityQueue struct {
	queue      []*Item
	comparator func(interface{}, interface{}) bool
}

func (pq PriorityQueue) Len() int { return len(pq.queue) }

func (pq PriorityQueue) Less(i, j int) bool {
	// We want Pop to give us the lowest, not greatest, priority so we use lower than here.
	return pq.comparator(pq.queue[i].Priority, pq.queue[j].Priority)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq.queue[i], pq.queue[j] = pq.queue[j], pq.queue[i]
	pq.queue[i].index = i
	pq.queue[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(pq.queue)
	item := x.(*Item)
	item.index = n
	pq.queue = append(pq.queue, item)
}

func (pq *PriorityQueue) Pop() interface{} {

	old := pq.queue
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	pq.queue = old[0 : n-1]
	return item
}

// update modifies the priority and value of an Item in the queue.
func (pq *PriorityQueue) update(item *Item, value interface{}, priority interface{}) {
	item.Value = value
	item.Priority = priority
	heap.Fix(pq, item.index)
}

func New(comparator func(interface{}, interface{}) bool) *PriorityQueue {
	pq := new(PriorityQueue)
	pq.comparator = comparator
	return pq
}
