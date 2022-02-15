package main

import (
    "context"
    "errors"
    "github.com/Maximilan4/rmq"
    "github.com/Maximilan4/rmq/presets"
    amqp "github.com/rabbitmq/amqp091-go"
    "log"
    "time"
)

func main() {
    ctx := context.Background()
    connection := rmq.NewDefaultConnection(ctx, "amqp://test:test@localhost:5672")
    err := connection.Connect(context.TODO())

    if err != nil {
        log.Fatal(err)
    }
    schema, err := connection.Schema()
    if err != nil {
        log.Fatal(err)
    }

    //declare exchange
    err = schema.Exchange.Declare(&rmq.DeclareParams{Name: "main_exchange", Kind: rmq.DirectExchange})
    if err != nil {
        log.Fatal(err)
    }

    //apply strategy to the current schema for working with rmq.DelayedRetryMessageHandler
    err = schema.ApplyPresets(presets.NewDelayedRetryStrategyPreset(
        "main_exchange",
        "main",
        &rmq.DeclareParams{Name: "main"},
        nil,
        nil,
    ),
    )

    if err != nil {
        log.Fatal(err)
    }

    //declare consumer
    consumer := rmq.NewConsumer(connection, &rmq.ConsumerConfig{
        WorkersCount: 3,
        Synchronous:  false,
    })

    //declare special handler
    handler := rmq.NewDelayedRetryMessageHandler(
        "main_exchange",
        "main.delay",
        time.Second*15,
        5,
        func(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) (rmq.MsgAction, error) {
            return rmq.ActionReject, errors.New("test err") // return an error for example of message movement
        },
    )

    //start consumer
    err = consumer.StartWorkersGroup(&rmq.ConsumeParams{
        Queue: "main",
    }, handler)

    if err != nil {
        log.Fatal(err)
    }
}
