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
    BindParams struct {
        Destination, Key, Source string
        NoWait                   bool
        Args                     amqp.Table
    }
    DeclareParams struct {
        Name string
        // Kind - for exchange only
        Kind                        ExchangeKind
        Durable, AutoDelete, NoWait bool
        // Internal - for exchange only
        Internal bool
        // Exclusive - for queue only
        Exclusive bool
        Args      amqp.Table
    }
    Schema struct {
        Queue    *QueueManager
        Exchange *ExchangeManager
    }
)

func (ek ExchangeKind) String() string {
    return string(ek)
}

func GetSchema(channel *amqp.Channel) *Schema {
    return &Schema{
        Queue: NewQueueManager(channel),
    }
}
