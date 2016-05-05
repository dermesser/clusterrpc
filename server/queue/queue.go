package queue

// An array-based fixed-length queue implementation, supposedly faster than a LinkedList implementation.
// Used for queuing both workers and requests.

type genericQueue struct {
	// tracking the length separately in l, because calculating it from (front, back)
	// is difficult in some cases (especially rollover)
	front, back, l int
	queue          []interface{}
}

func newQueue(len int) genericQueue {
	return genericQueue{front: 0, back: 0, l: 0, queue: make([]interface{}, len)}
}

func (q *genericQueue) len() int {
	return q.l
}

// Append to the back. Returns false if queue is full.
func (q *genericQueue) push(e interface{}) bool {
	if q.len() < len(q.queue) {
		q.queue[q.back] = e
		q.back = (q.back + 1) % len(q.queue)
		q.l++
		return true
	} else {
		return false
	}
}

// Get from the front. May return nil if queue is empty.
func (q *genericQueue) pop() interface{} {
	if q.len() > 0 {
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
func (q *genericQueue) peek() interface{} {
	if q.len() > 0 {
		return q.queue[q.front]
	} else {
		return false
	}
}
