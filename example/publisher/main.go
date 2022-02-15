package main

import (
    "context"
    "fmt"
    "github.com/Maximilan4/rmq"
    amqp "github.com/rabbitmq/amqp091-go"
    "log"
    "math/rand"
    "sync"
    "time"
)

func main() {
    mainCtx := context.Background()

    // init connection with main ctx inside
    connection := rmq.NewDefaultConnection(mainCtx, "amqp://test:test@localhost:5672")
    err := connection.Connect(context.TODO()) // connect to broker
    if err != nil {
        log.Fatal(err)
    }

    // create a new publisher instance
    publisher := rmq.NewPublisher(connection, &rmq.PublisherConfig{
        MaxChannelsCount: 10,
    })
    err = publisher.Init()

    if err != nil {
        log.Fatal(err)
    }

    wg := &sync.WaitGroup{}
    // publish 10k messages with random interval
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
                ExchangeName: "main_exchange",
                RoutingKey:   "main",
                Publishing: amqp.Publishing{
                    ContentType: "application/octet-stream",
                    Body:        []byte("test test"),
                },
            })
            if err != nil {
                fmt.Println(err)
                return
            }
        }(wg)
    }

    wg.Wait()
}
