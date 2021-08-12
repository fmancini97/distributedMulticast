package priorityqueue

import (
	"container/heap"
	"math/rand"
	"testing"
	"time"
)

func TestPriorityQueue(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	numValues := r.Intn(100)

	items := make(map[uint64]*Item)

	var pq heap.Interface
	pq = new(PriorityQueue)
	heap.Init(pq)

	for i := 0; i < numValues; i++ {
		priority := r.Uint64()
		_, ok := items[priority]
		for ok {
			priority = r.Uint64()
			_, ok = items[priority]
		}

		value := r.Intn(10000)
		item := new(Item)

		item.Priority = priority
		item.Value = value
		heap.Push(pq, item)
		items[priority] = item
	}

	// Check length
	if pq.Len() != numValues {
		t.Fatalf("Wrong number of items in the Priority Queue - Expected: %d Actual: %d", numValues, pq.Len())
	}

	priority := uint64(0)

	for i := 0; i < pq.Len(); i++ {
		actualItem := heap.Pop(pq).(*Item)

		if priority > actualItem.Priority {
			t.Fatalf("Wrong order: %d < %d", actualItem.Priority, priority)
		}

		priority = actualItem.Priority

		expectedItem, ok := items[actualItem.Priority]

		if !ok {
			t.Fatalf("Unexpected item with ptiority %d", actualItem.Priority)
		}

		if expectedItem.Priority != actualItem.Priority {
			t.Fatalf("Priority mismatch - Expected: %d, Actual: %d", expectedItem.Priority, actualItem.Priority)
		}

		if expectedItem.Value != actualItem.Value {
			t.Fatalf("Value mismatch - Expected: %d, Actual: %d", expectedItem.Value, actualItem.Value)
		}

	}
}
