// Copyright 2020-2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package write

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueue(t *testing.T) {
	que := NewQueue(2)
	assert.True(t, que.IsEmpty())
	assert.Nil(t, que.First())
	b := &Batch{Batch: "batch", RetryAttempts: 3}
	que.Push(b)
	assert.False(t, que.IsEmpty())
	//b2 := que.pop()
	//assert.Equal(t, b, b2)
	que.RemoveIfFirst(b)
	assert.True(t, que.IsEmpty())

	que.Push(b)
	que.Push(b)
	assert.True(t, que.Push(b))
	assert.False(t, que.IsEmpty())
	que.RemoveIfFirst(b)
	que.RemoveIfFirst(b)
	//que.pop()
	//que.pop()
	assert.True(t, que.IsEmpty())

}
