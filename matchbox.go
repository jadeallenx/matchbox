/*
Copyright 2015 Workiva

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
Package matchbox provides a concurrent pattern-matching engine designed for
high-throughput topic exchanges. It supports configurable wildcards and an
AMQP-compliant implementation.
*/
package matchbox

const (
	amqpSingleWildcard     = "*"
	amqpZeroOrMoreWildcard = "#"
	amqpDelimiter          = "."
)

// Subscriber is the value associated with a topic subscription.
type Subscriber interface {
	// ID returns a string which uniquely identifies the Subscriber.
	ID() string
}

// Config contains configuration parameters for a Matchbox such as wildcards
// and the word delimiter.
type Config struct {
	// SingleWildcard is a wildcard which matches exactly one word. For
	// example, if SingleWildcard is "*", "foo.*.baz" matches "foo.bar.baz" and
	// "foo.qux.baz" but not "foo.baz".
	SingleWildcard string

	// ZeroOrMoreWildcard is a wildcard which matches zero or more words. For
	// example, if ZeroOrMoreWildcard is "#", "foo.#.baz" matches "foo.baz",
	// "foo.bar.baz", and "foo.bar.qux.baz" but not "foo.bar".
	ZeroOrMoreWildcard string

	// Delimiter is the sequence which separates words. For example, if
	// Delimiter is ".", "foo.bar.baz" consists of the words "foo", "bar", and
	// "baz".
	Delimiter string
}

// reduceZeroOrMoreWildcards reduces sequences of zero-or-more wildcards,
// e.g. if zero-or-more wildcard is #, a.#.#.#.b reduces to a.#.b.
func (c *Config) reduceZeroOrMoreWildcards(words []string) []string {
	reduced := make([]string, 0, len(words))
	for i, word := range words {
		if word == c.ZeroOrMoreWildcard &&
			i+1 < len(words) && words[i+1] == c.ZeroOrMoreWildcard {
			continue
		}
		reduced = append(reduced, word)
	}
	return reduced
}

// NewAMQPConfig returns a Config which implements the AMQP specification for
// topic matching. Words are delimited by ".", single-word wildcards denoted by
// "*", and zero-or-more-word wildcards by "#".
func NewAMQPConfig() *Config {
	return &Config{
		SingleWildcard:     amqpSingleWildcard,
		ZeroOrMoreWildcard: amqpZeroOrMoreWildcard,
		Delimiter:          amqpDelimiter,
	}
}

// Matchbox handles topic subscription logic, including adding, removing, and
// performing lookups.
type Matchbox interface {
	// Subscribe a Subscriber to a topic.
	Subscribe(topic string, subscriber Subscriber)

	// Unsubscribe a Subscriber from a topic.
	Unsubscribe(topic string, subscriber Subscriber)

	// Subscribers returns the Subscribers for a topic.
	Subscribers(topic string) []Subscriber

	// Subscriptions returns a map of topics to Subscribers.
	Subscriptions() map[string][]Subscriber

	// Topics returns all of the currently contained topics.
	Topics() []string
}

// matchbox implements the Matchbox interface using a backing concurrent trie.
type matchbox struct {
	*ctrie
}

// NewMatchbox creates a new Matchbox with the given Config.
func New(config *Config) Matchbox {
	return &matchbox{newCtrie(config)}
}

// Subscribe a Subscriber to a topic.
func (m *matchbox) Subscribe(topic string, subscriber Subscriber) {
	m.Insert(topic, subscriber)
}

// Unsubscribe a Subscriber from a topic.
func (m *matchbox) Unsubscribe(topic string, subscriber Subscriber) {
	m.Remove(topic, subscriber)
}

// Subscribers returns the Subscribers for a topic.
func (m *matchbox) Subscribers(topic string) []Subscriber {
	return m.Lookup(topic)
}

// Subscriptions returns a map of topics to Subscribers.
func (m *matchbox) Subscriptions() map[string][]Subscriber {
	snapshot := m.ReadOnlySnapshot()
	subscriptions := map[string][]Subscriber{}
	root := snapshot.root.main.cNode
	for key, br := range root.branches {
		m.subscriptions(subscriptions, key, br)
	}
	return subscriptions
}

func (m *matchbox) subscriptions(subscriptions map[string][]Subscriber, path string, br *branch) {
	if len(br.subs) > 0 {
		subscriptions[path] = br.subscribers()
	}
	if br.iNode != nil && br.iNode.main.cNode != nil {
		for key, br := range br.iNode.main.cNode.branches {
			m.subscriptions(subscriptions, path+m.config.Delimiter+key, br)
		}
	}
}

// Topics returns all of the currently contained topics.
func (m *matchbox) Topics() []string {
	snapshot := m.ReadOnlySnapshot()
	topics := []string{}
	root := snapshot.root.main.cNode
	for key, br := range root.branches {
		topics = append(topics, m.topics(key, br)...)
	}
	return topics
}

func (m *matchbox) topics(path string, br *branch) []string {
	topics := []string{path}
	if br.iNode != nil && br.iNode.main.cNode != nil {
		for key, br := range br.iNode.main.cNode.branches {
			topics = append(topics, m.topics(path+m.config.Delimiter+key, br)...)
		}
	}
	return topics
}
