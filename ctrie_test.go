package matchbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type subscriber string

func (m subscriber) ID() string {
	return string(m)
}

func TestCtrie(t *testing.T) {
	assert := assert.New(t)
	ctrie := newCtrie(NewAMQPConfig())

	assert.Nil(ctrie.Lookup("foo"))

	sub1 := subscriber("abc")
	ctrie.Insert("foo", sub1)
	assert.Equal([]Subscriber{sub1}, ctrie.Lookup("foo"))

	ctrie.Insert("foo.bar", sub1)
	assert.Equal([]Subscriber{sub1}, ctrie.Lookup("foo.bar"))

	ctrie.Insert("bar.baz", sub1)
	assert.Equal([]Subscriber{sub1}, ctrie.Lookup("bar.baz"))

	sub2 := subscriber("def")
	ctrie.Insert("foo.bar", sub2)
	subs := ctrie.Lookup("foo.bar")
	if assert.Len(subs, 2) {
		for _, sub := range subs {
			assert.Contains([]Subscriber{sub1, sub2}, sub)
		}
	}
}
