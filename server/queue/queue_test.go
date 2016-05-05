package queue

import "testing"
import "container/list"

func TestPush(t *testing.T) {
	q := newQueue(3)

	a, b, c := q.push(3), q.push(4), q.push(5)

	if !(a && b && c) {
		t.Fatal("Could not push three elements")
	}
}

func TestPushLimit(t *testing.T) {
	q := newQueue(2)

	a, b, c := q.push(3), q.push(4), q.push(5)

	if !(a && b) && c {
		t.Fatal("Could push last element:", a, b, c)
	}
}

func TestPopEmpty(t *testing.T) {
	q := newQueue(10)

	if nil != q.pop() {
		t.Fatal("Could pop from empty queue")
	}
}

func TestPop(t *testing.T) {
	q := newQueue(10)

	q.push(1)
	q.push(2)
	q.push(3)

	a, b, c := q.pop().(int), q.pop().(int), q.pop().(int)

	if a != 1 || b != 2 || c != 3 {
		t.Fatal("Bad contents:", a, b, c)
	}

	if nil != q.pop() {
		t.Fatal("Yields element past end")
	}
}

func TestLen(t *testing.T) {
	q := newQueue(10)
	q.push(2)
	q.push(3)
	if q.len() != 2 {
		t.Fatal("Wrong length", q.len())
	}
}
func TestLen2(t *testing.T) {
	q := newQueue(3)
	q.push(2)
	q.push(3)
	q.push(4)
	q.pop()
	q.pop()
	q.push(5)
	if q.len() != 2 {
		t.Fatal("Wrong length", q.len())
	}
	q.pop()
	if q.pop().(int) != 5 {
		t.Fatal("Unexpected value")
	}
}

func TestPeek(t *testing.T) {
	q := newQueue(10)
	q.push(2)

	if 2 != q.peek().(int) {
		t.Fatal("Wrong element:", q.peek().(int))
	}
}

// Benches time for pushing, then popping 10 elements from a long queue
func BenchmarkQueue(b *testing.B) {
	q := newQueue(1000)

	for i := 0; i < b.N; i++ {
		for j := 0; j < 10; j++ {
			if !q.push(j) {
				b.Fatal("couldn't push")
			}
		}
		for j := 0; j < 10; j++ {
			if nil == q.pop() {
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
				b.Fatal("couldn't push")
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
