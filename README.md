# matchbox

Matchbox is a concurrent pattern-matching engine designed for high-throughput topic exchanges. It supports configurable wildcards and an AMQP-compliant implementation.

```go
type subscriber string

func (s subscriber) ID() string {
		return string(s)
}

var (
		forex  = subscriber("forex")
		stock  = subscriber("stock")
		nasdaq = subscriber("nasdaq")
		nyse   = subscriber("nyse")
		tech   = subscriber("tech")
)

mb := matchbox.New(matchbox.NewAMQPConfig())

mb.Subscribe("PRICE.STOCK.NASDAQ.MSFT", tech)
mb.Subscribe("PRICE.STOCK.*.AAPL", tech)
mb.Subscribe("PRICE.STOCK.NYSE.*", nyse)
mb.Subscribe("PRICE.STOCK.NASDAQ.*", nasdaq)
mb.Subscribe("PRICE.STOCK.*.*", stock)
mb.Subscribe("EUR.STOCK.DB", forex)
mb.Subscribe("USD.#", forex)

for _, sub := range mb.Subscribers("PRICE.STOCK.NYSE.IBM") {
		fmt.Println("PRICE.STOCK.NYSE.IBM", sub.ID())
}
```

## Wildcards

Two wildcard types are supported: single-word and zero-or-more-words. In AMQP, these are `*` and `#`, respectively. In this case, `*` matches exactly one word, while `#` matches zero or more words. For example, `*.stock.#` matches the `usd.stock` and `eur.stock.db` but not `stock.nasdaq`.
