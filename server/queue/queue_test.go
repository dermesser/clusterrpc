package queue

import "testing"
import "container/list"

func TestPush(t *testing.T) {
	q := NewQueue(3)

	a, b, c := q.Push(3), q.Push(4), q.Push(5)

	if !(a && b && c) {
		t.Fatal("Could not Push three elements")
	}
}

func TestPushLimit(t *testing.T) {
	q := NewQueue(2)

	a, b, c := q.Push(3), q.Push(4), q.Push(5)

	if !(a && b) && c {
		t.Fatal("Could Push last element:", a, b, c)
	}
}

func TestPopEmpty(t *testing.T) {
	q := NewQueue(10)

	if nil != q.Pop() {
		t.Fatal("Could Pop from empty queue")
	}
}

func TestPop(t *testing.T) {
	q := NewQueue(10)

	q.Push(1)
	q.Push(2)
	q.Push(3)

	a, b, c := q.Pop().(int), q.Pop().(int), q.Pop().(int)

	if a != 1 || b != 2 || c != 3 {
		t.Fatal("Bad contents:", a, b, c)
	}

	if nil != q.Pop() {
		t.Fatal("Yields element past end")
	}
}

func TestLen(t *testing.T) {
	q := NewQueue(10)
	q.Push(2)
	q.Push(3)
	if q.Len() != 2 {
		t.Fatal("Wrong length", q.Len())
	}
}
func TestLen2(t *testing.T) {
	q := NewQueue(3)
	q.Push(2)
	q.Push(3)
	q.Push(4)
	q.Pop()
	q.Pop()
	q.Push(5)
	if q.Len() != 2 {
		t.Fatal("Wrong length", q.Len())
	}
	q.Pop()
	if q.Pop().(int) != 5 {
		t.Fatal("Unexpected value")
	}
}

func TestPeek(t *testing.T) {
	q := NewQueue(10)
	q.Push(2)

	if 2 != q.peek().(int) {
		t.Fatal("Wrong element:", q.peek().(int))
	}
}

// Benches time for Pushing, then Popping 10 elements from a long queue
func BenchmarkQueue(b *testing.B) {
	q := NewQueue(1000)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			if !q.Push(j) {
				b.Fatal("couldn't Push")
			}
		}
		for j := 0; j < 10; j++ {
			if nil == q.Pop() {
				b.Fatal("got nil", i, j)
			}
		}
	}
}

// Compare with linked list performance
func BenchmarkQueueLinkedList(b *testing.B) {
	l := list.New()

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			if nil == l.PushBack(i) {
				b.Fatal("couldn't Push")
			}
		}
		for j := 0; j < 10; j++ {
			if nil == l.Remove(l.Front()) {
				b.Fatal("got nil", i, j)
			}
		}
	}
}

// Compare with chans
func BenchmarkQueueChan(b *testing.B) {
	c := make(chan int, 1000)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			c <- i
		}
		for j := 0; j < 10; j++ {
			if -1 == <-c {
				b.Fatal("unexpected value")
			}
		}
	}
}
