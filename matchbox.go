package matchbox

const (
	amqpSingleWildcard     = "*"
	amqpZeroOrMoreWildcard = "#"
	amqpDelimiter          = "."
)

// Subscriber is the value associated with a topic subscription.
type Subscriber interface {
	ID() string
}

// Config contains configuration parameters for a Matchbox such as wildcards
// and the word delimiter.
type Config struct {
	SingleWildcard     string
	ZeroOrMoreWildcard string
	Delimiter          string
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
	Subscribe(subscriber Subscriber, topic string)

	// Unsubscribe a Subscriber from a topic.
	Unsubscribe(subscriber Subscriber, topic string)

	// Subscribers returns the Subscribers for a topic.
	Subscribers(topic string) []Subscriber
}

type matchbox struct {
	config *Config
}

func NewMatchbox(config *Config) Matchbox {
	return &matchbox{config}
}

// Subscribe a Subscriber to a topic.
func (m *matchbox) Subscribe(subscriber Subscriber, topic string) {
	// TODO
}

// Unsubscribe a Subscriber from a topic.
func (m *matchbox) Unsubscribe(subscriber Subscriber, topic string) {
	// TODO
}

// Subscribers returns the Subscribers for a topic.
func (m *matchbox) Subscribers(topic string) []Subscriber {
	// TODO
	return nil
}
