package main

import (
    "context"
    "fmt"
    "github.com/Maximilan4/rmq"
    "github.com/streadway/amqp"
    "log"
    "math/rand"
    "sync"
    "time"
)

func main() {
    mainCtx := context.Background()
    publisher := rmq.NewPublisher(mainCtx, &rmq.PublisherConfig{
        Dsn:              "amqp://user:pass@localhost:5672",
        MaxChannelsCount: 10,
    })

    ctx, done := context.WithTimeout(mainCtx, time.Second*30)
    err := publisher.Connect(ctx)
    done()
    if err != nil {
        log.Fatal(err)
    }

    wg := &sync.WaitGroup{}
    for i := 0; i < 10000; i++ {
        wg.Add(1)

        go func(wg *sync.WaitGroup) {
            defer wg.Done()
            time.Sleep(time.Duration(rand.Int()/1000000000) * time.Nanosecond)
            if err != nil {
                fmt.Println(err)
                return
            }
            err = publisher.Publish(mainCtx, &rmq.PublishMessage{
                ExchangeName: "test",
                RoutingKey:   "",
                Publishing: amqp.Publishing{
                    ContentType: "application/octet-stream",
                    Body:        []byte("test test"),
                },
            })
            if err != nil {
                fmt.Println(err)
                return
            }

            err = publisher.Publish(mainCtx, &rmq.PublishMessage{
                ExchangeName: "test2",
                RoutingKey:   "",
                Publishing: amqp.Publishing{
                    ContentType: "application/octet-stream",
                    Body:        []byte("test 2"),
                },
            })
            if err != nil {
                fmt.Println(err)
            }
        }(wg)
    }

    wg.Wait()
}
