package main

import (
    "context"
    "fmt"
    "github.com/Maximilan4/rmq"
    amqp "github.com/rabbitmq/amqp091-go"
    "golang.org/x/sync/errgroup"
    "log"
    "time"
)

func main() {
    ctx := context.Background()

    connection := rmq.NewDefaultConnection(ctx, "amqp://test:test@localhost:5672", &rmq.ConnectionCfg{ReconnectTimeout: time.Minute})
    tCtx, done := context.WithTimeout(ctx, 30*time.Second)
    err := connection.Connect(tCtx)

    if err != nil {
        log.Fatal(err)
    }
    done()

    consumer := rmq.NewConsumer(connection, &rmq.ConsumerConfig{
        WorkersCount: 3,
        Synchronous:  false,
    })

    handler := rmq.NewDefaultMessageHandler(func(ctx context.Context, msg *amqp.Delivery) (rmq.MsgAction, error) {
        fmt.Println(msg.Body)
        return rmq.ActionAck, nil
    })

    group, _ := errgroup.WithContext(ctx)
    group.Go(func() error {
        return consumer.StartWorkersGroup(&rmq.ConsumeParams{
            Queue: "test",
        }, handler)
    })

    group.Go(func() error {
        return consumer.StartWorkersGroup(&rmq.ConsumeParams{
            Queue: "test2",
        }, handler)
    })

    if err = group.Wait(); err != nil {
        log.Fatal(err)
    }

}
