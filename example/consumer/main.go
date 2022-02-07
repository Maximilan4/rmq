package main

import (
    "context"
    "fmt"
    "github.com/Maximilan4/rmq"
    "github.com/streadway/amqp"
    "golang.org/x/sync/errgroup"
    "log"
    "time"
)

func main() {
    ctx := context.Background()
    consumer := rmq.NewConsumer(ctx, &rmq.ConsumerConfig{
        Dsn:          "amqp://user:user@localhost:5672",
        WorkersCount: 3,
        Synchronous:  false,
    })

    ctx, done := context.WithTimeout(ctx, 10*time.Second)
    err := consumer.Connect(ctx)
    if err != nil {
        log.Fatal(err)
    }
    done()

    handler := rmq.NewDefaultMessageHandler(func(ctx context.Context, msg *amqp.Delivery) (rmq.MsgAction, error) {
        fmt.Println(msg.Body)
        return rmq.ActionAck, nil
    })

    group, ctx := errgroup.WithContext(ctx)
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
