package rmq

import (
    amqp "github.com/rabbitmq/amqp091-go"
)

const (
    DirectExchange  ExchangeKind = "direct"
    FanoutExchange  ExchangeKind = "fanout"
    TopicExchange   ExchangeKind = "topic"
    HeadersExchange ExchangeKind = "headers"
)

type (
    ExchangeKind string
    DeleteParams struct {
        Name                      string
        IfUnused, IfEmpty, NoWait bool
    }
    DeclareParams struct {
        Name string
        // Kind - for exchange only
        Kind                                 ExchangeKind
        Durable, AutoDelete, NoWait, Passive bool
        // Internal - for exchange only
        Internal bool
        // Exclusive - for queue only
        Exclusive bool
        Args      amqp.Table
    }
    Schema struct {
        Queue    *QueueManager
        Exchange *ExchangeManager
        channel  *amqp.Channel
    }
    Preset interface {
        Apply(*amqp.Channel, *Schema) error
    }
)

func (sc *Schema) ApplyPresets(presets ...Preset) (err error) {
    for _, preset := range presets {
        if err = preset.Apply(sc.channel, sc); err != nil {
            return
        }
    }

    return
}

func (ek ExchangeKind) String() string {
    return string(ek)
}

func (dp *DeclareParams) WithDeadLetterExchange(name string) *DeclareParams {
    if dp.Args == nil {
        dp.Args = make(amqp.Table)
    }

    dp.Args["x-dead-letter-exchange"] = name

    return dp
}

func (dp *DeclareParams) WithDeadLetterRk(key string) *DeclareParams {
    if dp.Args == nil {
        dp.Args = make(amqp.Table)
    }

    dp.Args["x-dead-letter-routing-key"] = key

    return dp
}

func GetSchema(channel *amqp.Channel) *Schema {
    return &Schema{
        Queue:    NewQueueManager(channel),
        Exchange: NewExchangeManager(channel),
        channel:  channel,
    }
}
