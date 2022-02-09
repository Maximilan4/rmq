package schema

import (
    amqp "github.com/rabbitmq/amqp091-go"
)

type (
    BindParams struct {
        Destination, Key, Source string
        NoWait                   bool
        Args                     amqp.Table
    }
    DeclareParams struct {
        Name string
        // Kind - for exchange only
        Kind                        string
        Durable, AutoDelete, NoWait bool
        // Exclusive - for queue only
        Exclusive bool
        Args      amqp.Table
    }
    Schema struct {
        Queue *QueueManager
    }
)

func Get(channel *amqp.Channel) *Schema {
    return &Schema{
        Queue: NewQueueManager(channel),
    }
}
