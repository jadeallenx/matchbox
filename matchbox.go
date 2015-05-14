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
	SingleWildcard     string
	ZeroOrMoreWildcard string
	Delimiter          string
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
// topic matching.
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
