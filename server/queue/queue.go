package queue

// An array-based fixed-length queue implementation, supposedly faster than a LinkedList implementation.
// Used for queuing both workers and requests.

type Queue struct {
	// tracking the length separately in l, because calculating it from (front, back)
	// is difficult in some cases (especially rollover)
	front, back, l int
	queue          []interface{}
}

func NewQueue(len int) Queue {
	return Queue{front: 0, back: 0, l: 0, queue: make([]interface{}, len)}
}

func (q *Queue) Len() int {
	return q.l
}

// Append to the back. Returns false if queue is full.
func (q *Queue) Push(e interface{}) bool {
	if q.Len() < len(q.queue) {
		q.queue[q.back] = e
		q.back = (q.back + 1) % len(q.queue)
		q.l++
		return true
	} else {
		return false
	}
}

// Get from the front. May return nil if queue is empty.
func (q *Queue) Pop() interface{} {
	if q.Len() > 0 {
		e := q.queue[q.front]
		q.queue[q.front] = nil
		q.front = (q.front + 1) % len(q.queue)
		q.l--
		return e
	} else {
		return nil
	}
}

// Returns the front element without removing it.
func (q *Queue) peek() interface{} {
	if q.Len() > 0 {
		return q.queue[q.front]
	} else {
		return false
	}
}
