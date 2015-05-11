package matchbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type subscriber string

func (m subscriber) ID() string {
	return string(m)
}

func TestSubscriptionManager(t *testing.T) {
	assert := assert.New(t)
	mb := New(NewAMQPConfig())
	sub1 := subscriber("abc")
	sub2 := subscriber("def")
	sub3 := subscriber("ghi")
	sub4 := subscriber("jkl")

	assert.Equal([]Subscriber{}, mb.Subscribers("foo"))
	mb.Unsubscribe("moo", sub1)

	mb.Subscribe("a", sub1)
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("a"))
	mb.Unsubscribe("a", sub1)
	assert.Equal([]Subscriber{}, mb.Subscribers("a"))
	mb.Subscribe("a", sub1)
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("a"))
	mb.Subscribe("a", sub2)
	assert.Equal(2, len(mb.Subscribers("a")))

	mb.Subscribe("a.b.c", sub1)
	mb.Subscribe("a.*.c", sub2)
	mb.Subscribe("*.*.c", sub3)
	mb.Subscribe("*.*.*", sub4)
	subscribers := mb.Subscribers("a.b.c")
	sessions := []Subscriber{sub1, sub2, sub3, sub4}
	assert.Len(subscribers, 4)
	for _, subscriber := range subscribers {
		assert.Contains(sessions, subscriber)
	}
	mb.Unsubscribe("a.b.c", sub1)
	sessions = []Subscriber{sub2, sub3, sub4}
	subscribers = mb.Subscribers("a.b.c")
	assert.Len(subscribers, 3)
	for _, subscriber := range subscribers {
		assert.Contains(sessions, subscriber)
	}
	mb.Unsubscribe("a.*.c", sub2)
	sessions = []Subscriber{sub3, sub4}
	subscribers = mb.Subscribers("a.b.c")
	assert.Len(subscribers, 2)
	for _, subscriber := range subscribers {
		assert.Contains(sessions, subscriber)
	}
	mb.Unsubscribe("*.*.c", sub3)
	assert.Equal([]Subscriber{sub4}, mb.Subscribers("a.b.c"))
	mb.Unsubscribe("*.*.*", sub4)
	assert.Equal([]Subscriber{}, mb.Subscribers("a.b.c"))

	mb.Subscribe("a.b.b.c", sub2)
	mb.Subscribe("a.*.*.c", sub3)
	subscribers = mb.Subscribers("a.b.b.c")
	sessions = []Subscriber{sub2, sub3}
	assert.Len(subscribers, 2)
	for _, subscriber := range subscribers {
		assert.Contains(sessions, subscriber)
	}
	assert.Equal([]Subscriber{sub3}, mb.Subscribers("a.b.x.c"))
	mb.Unsubscribe("a.b", sub2)
	mb.Unsubscribe("a.b.b.c", sub2)
	mb.Unsubscribe("a.b.b.c", sub2)
	assert.Equal([]Subscriber{sub3}, mb.Subscribers("a.b.b.c"))

	mb.Subscribe("d.#.f", sub1)
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("d.f"))
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("d.e.f"))
	assert.Equal([]Subscriber{sub1}, mb.Subscribers("d.e.e.e.e.e.f"))

	mb.Subscribe("x.#", sub3)
	assert.Equal([]Subscriber{sub3}, mb.Subscribers("x"))
	assert.Equal([]Subscriber{sub3}, mb.Subscribers("x.y"))
	subscribers = mb.Subscribers("x.y.z")
	sessions = []Subscriber{sub3, sub4}
	for _, subscriber := range subscribers {
		assert.Contains(sessions, subscriber)
	}
	assert.Equal([]Subscriber{sub3}, mb.Subscribers("x.y.z.z.z.z.z.z.z"))

	mb.Subscribe("x.#.#.#.y.z", sub4)
	assert.Equal([]Subscriber{sub4}, mb.Subscribers("x.a.y.z"))
	assert.Equal([]Subscriber{sub4}, mb.Subscribers("x.a.a.a.y.z"))
	assert.Equal([]Subscriber{}, mb.Subscribers("x.a.a.a.y"))
	mb.Unsubscribe("x.#.#.#.y.z", sub4)
	assert.Equal([]Subscriber{}, mb.Subscribers("x.a.y.z"))
	assert.Equal([]Subscriber{}, mb.Subscribers("x.a.a.a.y.z"))
	assert.Equal([]Subscriber{}, mb.Subscribers("x.a.a.a.y"))
}

// Ensures reduceZeroOrMoreWildcards reduces sequences of # to a single
// instance.
func TestReduceZeroOrMoreWildcards(t *testing.T) {
	assert := assert.New(t)
	config := NewAMQPConfig()
	words := []string{"a", "b", "c", "d"}
	assert.Equal(words, config.reduceZeroOrMoreWildcards(words))
	words = []string{"a", "#", "c", "d"}
	assert.Equal(words, config.reduceZeroOrMoreWildcards(words))
	words = []string{"a", "#", "#", "d"}
	assert.Equal([]string{"a", "#", "d"}, config.reduceZeroOrMoreWildcards(words))
	words = []string{"a", "#", "#", "#"}
	assert.Equal([]string{"a", "#"}, config.reduceZeroOrMoreWildcards(words))
	words = []string{"a", "#", "#", "b", "#", "c", "#", "#", "#", "d"}
	assert.Equal([]string{"a", "#", "b", "#", "c", "#", "d"},
		config.reduceZeroOrMoreWildcards(words))
}

func BenchmarkSubscribeSingleChild(b *testing.B) {
	mb := New(NewAMQPConfig())
	sub := subscriber("abc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mb.Subscribe("a", sub)
	}
}

func BenchmarkSubscribeLongBranch(b *testing.B) {
	mb := New(NewAMQPConfig())
	sub := subscriber("abc")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mb.Subscribe("a.b.c.d.e.f.g.h", sub)
	}
}

func BenchmarkSubscribeFanOutChild(b *testing.B) {
	mb := New(NewAMQPConfig())
	sub := subscriber("abc")
	mb.Subscribe("a", sub)
	mb.Subscribe("b", sub)
	mb.Subscribe("c", sub)
	mb.Subscribe("d", sub)
	mb.Subscribe("e", sub)
	mb.Subscribe("f", sub)
	mb.Subscribe("g", sub)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mb.Subscribe("g.h", sub)
	}
}
