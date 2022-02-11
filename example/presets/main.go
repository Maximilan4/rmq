package main

import (
    "context"
    "github.com/Maximilan4/rmq"
    "github.com/Maximilan4/rmq/presets"
    "log"
)

func main() {
    ctx := context.Background()
    connection := rmq.NewDefaultConnection(ctx, "amqp://test:test@localhost:5672", &rmq.ConnectionCfg{})
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

    err = schema.ApplyPresets(presets.NewFallbackStrategyPreset(
        "test-exchange",
        "main",
        &rmq.DeclareParams{Name: "main"}, nil, nil),
    )

    if err != nil {
        log.Fatal(err)
    }
}
