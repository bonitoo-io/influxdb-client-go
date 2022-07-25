// Copyright 2020-2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package write

import (
	"container/list"
)

type Queue struct {
	list  *list.List
	limit int
}

func NewQueue(limit int) *Queue {
	return &Queue{list: list.New(), limit: limit}
}
func (q *Queue) Push(batch *Batch) bool {
	overWrite := false
	if q.list.Len() == q.limit {
		q.list.Remove(q.list.Front())
		overWrite = true
	}
	q.list.PushBack(batch)
	return overWrite
}

func (q *Queue) RemoveIfFirst(batch *Batch) {
	el := q.list.Front()
	if el != nil && el.Value == batch {
		q.list.Remove(el)
	}
}

func (q *Queue) First() *Batch {
	el := q.list.Front()
	if el != nil {
		return el.Value.(*Batch)
	}
	return nil
}

func (q *Queue) IsEmpty() bool {
	return q.list.Len() == 0
}

func (q *Queue) Len() int {
	return q.list.Len()
}
