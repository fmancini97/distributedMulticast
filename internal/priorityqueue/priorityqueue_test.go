package priorityqueue

import (
	"container/heap"
	"gitlab.com/f_mancini/distributedMulticast/internal/randstring"
	"math/rand"
	"testing"
	"time"
)

func TestPriorityQueue_SingleLevelPriority(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	numValues := r.Intn(100)

	items := make(map[uint64]*Item)

	var pq heap.Interface
	pq = New(func(i interface{}, i2 interface{}) bool {
		return i.(uint64) < i2.(uint64)
	})
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

		if priority > actualItem.Priority.(uint64) {
			t.Fatalf("Wrong order: %d < %d", actualItem.Priority, priority)
		}

		priority = actualItem.Priority.(uint64)

		expectedItem, ok := items[actualItem.Priority.(uint64)]

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

type twoLevelPriority struct {
	firstLevel  uint64
	secondLevel string
}

func TestPriorityQueue_TwoLevelPriority(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	numValues := r.Intn(100)

	if numValues%2 == 1 {
		numValues++
	}

	items := make(map[twoLevelPriority]*Item)

	var pq heap.Interface
	pq = New(func(i interface{}, i2 interface{}) bool {
		tlp1 := i.(twoLevelPriority)
		tlp2 := i2.(twoLevelPriority)
		if tlp1.firstLevel == tlp2.firstLevel {
			return tlp1.secondLevel < tlp2.secondLevel
		} else {
			return tlp1.firstLevel < tlp2.firstLevel
		}

	})
	heap.Init(pq)

	for i := 0; i < numValues; i = i + 2 {
		firstLevelValue := r.Uint64()
		for j := 0; j < 2; j++ {
			priority := twoLevelPriority{
				firstLevel:  firstLevelValue,
				secondLevel: randstring.RandSeq(r.Intn(50)),
			}
			_, ok := items[priority]
			for ok {
				priority = twoLevelPriority{
					firstLevel:  firstLevelValue,
					secondLevel: randstring.RandSeq(r.Intn(50)),
				}
				_, ok = items[priority]
			}

			value := r.Intn(10000)
			item := new(Item)

			item.Priority = priority
			item.Value = value
			heap.Push(pq, item)
			items[priority] = item
		}
	}

	// Check length
	if pq.Len() != numValues {
		t.Fatalf("Wrong number of items in the Priority Queue - Expected: %d Actual: %d", numValues, pq.Len())
	}

	priority := twoLevelPriority{}

	for i := 0; i < pq.Len(); i++ {
		actualItem := heap.Pop(pq).(*Item)

		actualPriority := actualItem.Priority.(twoLevelPriority)

		if priority.firstLevel == actualPriority.firstLevel && priority.secondLevel > actualPriority.secondLevel {
			t.Fatalf("Wrong order: %s < %s", actualPriority.secondLevel, priority.secondLevel)
		} else if priority.firstLevel > actualPriority.firstLevel {
			t.Fatalf("Wrong order: %d < %d", actualPriority.firstLevel, priority.firstLevel)
		}

		priority = actualPriority

		expectedItem, ok := items[actualPriority]

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
