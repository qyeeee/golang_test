package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Queue struct {
	items   []string
	waiters []chan string
}

func (q *Queue) push(msg string) {
	if len(q.waiters) > 0 {
		q.waiters[0] <- msg
		q.waiters = q.waiters[1:]
	} else {
		q.items = append(q.items, msg)
	}
}

func (q *Queue) pop() (string, bool) {
	if len(q.items) == 0 {
		return "", false
	}
	msg := q.items[0]
	q.items = q.items[1:]
	return msg, true
}

type Broker struct {
	mu     sync.Mutex
	queues map[string]*Queue
}

func (b *Broker) getOrCreate(name string) *Queue {
	q, ok := b.queues[name]
	if !ok {
		q = &Queue{}
		b.queues[name] = q
	}
	return q
}

func (b *Broker) Push(name, msg string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.getOrCreate(name).push(msg)
}

func (b *Broker) Pop(name string, timeout time.Duration) (string, bool) {
	b.mu.Lock()
	q := b.getOrCreate(name)
	if msg, ok := q.pop(); ok {
		b.mu.Unlock()
		return msg, true
	}
	if timeout == 0 {
		b.mu.Unlock()
		return "", false
	}
	ch := make(chan string, 1)
	q.waiters = append(q.waiters, ch)
	time.AfterFunc(timeout, func() {
		b.mu.Lock()
		for i, w := range q.waiters {
			if w == ch {
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		b.mu.Unlock()
		close(ch)
	})
	b.mu.Unlock()
	msg, ok := <-ch
	return msg, ok
}

func (b *Broker) Handler(w http.ResponseWriter, r *http.Request) {
	queueName := strings.Trim(r.URL.Path, "/")
	if queueName == "" {
		http.Error(w, "", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		v := r.URL.Query().Get("v")
		if !r.URL.Query().Has("v") {
			http.Error(w, "", http.StatusBadRequest)
			return
		}
		b.Push(queueName, v)
	case http.MethodGet:
		timeoutStr := r.URL.Query().Get("timeout")
		var timeout time.Duration
		if timeoutStr != "" {
			if t, err := strconv.Atoi(timeoutStr); err == nil && t > 0 {
				timeout = time.Duration(t) * time.Second
			}
		}
		msg, ok := b.Pop(queueName, timeout)
		if !ok {
			http.Error(w, "", http.StatusNotFound)
			return
		}
		fmt.Fprint(w, msg)
		return
	default:
		http.Error(w, "", http.StatusMethodNotAllowed)
	}
}

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	flag.Parse()
	broker := &Broker{queues: make(map[string]*Queue)}
	http.HandleFunc("/", broker.Handler)
	http.ListenAndServe(fmt.Sprintf(":%d", *port), nil)
}
