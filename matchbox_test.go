package matchbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type subscriber string

func (m subscriber) ID() string {
	return string(m)
}

func TestBasicOperations(t *testing.T) {
	assert := assert.New(t)
	mb := NewMatchbox(NewAMQPConfig())

	assert.Nil(mb.Subscribers("foo"))

	sub1 := subscriber("abc")
	mb.Subscribe("foo", sub1)
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("foo"))

	mb.Subscribe("foo.bar", sub1)
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("foo.bar"))

	mb.Subscribe("bar.baz", sub1)
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("bar.baz"))

	sub2 := subscriber("def")
	mb.Subscribe("foo.bar", sub2)
	subs := mb.Subscribers("foo.bar")
	if assert.Len(subs, 2) {
		for _, sub := range subs {
			assert.Contains([]Subscriber{sub1, sub2}, sub)
		}
	}
}
