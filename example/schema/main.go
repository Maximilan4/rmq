package main

import (
    "context"
    "github.com/Maximilan4/rmq"
    "log"
    "time"
)

func main() {
    ctx := context.Background()
    connection := rmq.NewDefaultConnection(ctx, "amqp://test:test@localhost:5672", &rmq.ConnectionCfg{
        ReconnectTimeout: time.Minute,
    })
    err := connection.Connect(context.TODO())

    if err != nil {
        log.Fatal(err)
    }
    schema, err := connection.Schema()
    if err != nil {
        log.Fatal(err)
    }

    err = schema.Exchange.Declare(&rmq.DeclareParams{Name: "test-exchange", Kind: rmq.DirectExchange})
    if err != nil {
        log.Fatal(err)
    }

    err = schema.Queue.DeclareMulti(&rmq.DeclareParams{Name: "test-q1"}, &rmq.DeclareParams{Name: "test-q2"})
    if err != nil {
        log.Fatal(err)
    }

    err = schema.Queue.BindMulti(
        &rmq.BindParams{Destination: "test-q1", Key: "rk1"},
        &rmq.BindParams{Destination: "test-q2", Key: "rk1"},
    )
    if err != nil {
        log.Fatal(err)
    }
}
