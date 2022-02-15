[![Go Report Card](https://goreportcard.com/badge/github.com/Maximilan4/rmq)](https://goreportcard.com/report/github.com/Maximilan4/rmq)
[![Go Reference](https://pkg.go.dev/badge/github.com/Maximilan4/rmq.svg)](https://pkg.go.dev/github.com/Maximilan4/rmq)
[![License: Unlicense](https://img.shields.io/badge/license-Unlicense-blue.svg)](http://unlicense.org/)

## RMQ:
Library provide some wrappers of `github.com/rabbitmq/amqp091-go`

### Connection:
```golang
connection := rmq.NewDefaultConnection(context.Background(), "amqp://test:test@localhost:5672")
// or use rmq.NewConnection with callback for construct connection with options
err := connection.Connect(context.TODO())

if err != nil {
	log.Fatal(err)
}

```
### Working with schema:
```golang
schema, err := connection.Schema() // creates a new schema with separate channel inside
if err != nil {
	log.Fatal(err)
}

// declare an exchange
err = schema.Exchange.Declare(&rmq.DeclareParams{Name: "test-exchange", Kind: rmq.DirectExchange})
if err != nil {
	log.Fatal(err)
}

// declare two queue
err = schema.Queue.DeclareMulti(&rmq.DeclareParams{Name: "test-q1"}, &rmq.DeclareParams{Name: "test-q2"})
if err != nil {
	log.Fatal(err)
}
// bind queues to exchange
err = schema.Queue.BindMulti(
	&rmq.QueueBindParams{Name: "test-q1", Key: "rk1", Exchange: "test-exchange"},
	&rmq.QueueBindParams{Name: "test-q2", Key: "rk1", Exchange: "test-exchange"},
)

if err != nil {
	log.Fatal(err)
}
```
### Consumer:
```golang
consumer := rmq.NewConsumer(connection, &rmq.ConsumerConfig{
		WorkersCount: 3, // 3 workers goroutine will be started
		Synchronous:  false, // run handler in single goroutine or not
	})
//define a message handler (use defaults or write own)
handler := rmq.NewDefaultMessageHandler(func(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) (rmq.MsgAction, error) {
	fmt.Println(msg.Body)
	return rmq.ActionAck, nil
})
// start worker
err := consumer.StartWorkersGroup(&rmq.ConsumeParams{Queue: "test"}, handler)
// or use consumer.StartWorker(...) for single consuming process
if err != nil {
	log.Fatal(err)
}
```
### Publisher:
```golang
// create a new publisher instance
publisher := rmq.NewPublisher(connection, &rmq.PublisherConfig{
	MaxChannelsCount: 10, // max pool channels count
})
err = publisher.Init()

if err != nil {
	log.Fatal(err)
}
err = publisher.Publish(context.TODO(), &rmq.PublishMessage{
				ExchangeName: "main_exchange",
				RoutingKey:   "main",
				Publishing: amqp.Publishing{
					ContentType: "application/octet-stream",
					Body:        []byte("test test"),
				},
})

if err != nil {
	log.Fatal(err)
}
