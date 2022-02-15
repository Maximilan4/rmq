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
    //ExchangeKind - type for exchange kind
    ExchangeKind string
    //DeleteParams - common deletion params
    DeleteParams struct {
        Name                      string
        IfUnused, IfEmpty, NoWait bool
    }
    //DeclareParams - common declare params
    DeclareParams struct {
        //Primitive name
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
    //Schema - struct for manager`s access
    Schema struct {
        Queue    *QueueManager
        Exchange *ExchangeManager
        channel  *amqp.Channel
    }
    //Preset - interface for apply any set of params to schema
    Preset interface {
        Apply(*amqp.Channel, *Schema) error
    }
)

//ApplyPresets - apply presets to schema
func (sc *Schema) ApplyPresets(presets ...Preset) (err error) {
    for _, preset := range presets {
        if err = preset.Apply(sc.channel, sc); err != nil {
            return
        }
    }

    return
}

//String - conversion ExchangeKind to string
func (ek ExchangeKind) String() string {
    return string(ek)
}

//WithDeadLetterExchange - sets `x-dead-letter-exchange` param
func (dp *DeclareParams) WithDeadLetterExchange(name string) *DeclareParams {
    if dp.Args == nil {
        dp.Args = make(amqp.Table)
    }

    dp.Args["x-dead-letter-exchange"] = name

    return dp
}

//WithDeadLetterRk - sets `x-dead-letter-routing-key` param
func (dp *DeclareParams) WithDeadLetterRk(key string) *DeclareParams {
    if dp.Args == nil {
        dp.Args = make(amqp.Table)
    }

    dp.Args["x-dead-letter-routing-key"] = key

    return dp
}

//GetSchema - creates a new Schema instance
func GetSchema(channel *amqp.Channel) *Schema {
    return &Schema{
        Queue:    NewQueueManager(channel),
        Exchange: NewExchangeManager(channel),
        channel:  channel,
    }
}
