package rmq

import (
    "fmt"
    amqp "github.com/rabbitmq/amqp091-go"
)

type (
    ExchangeBindParams struct {
        Destination, Key, Source string
        NoWait                   bool
        Args                     amqp.Table
    }
    ExchangeManager struct {
        channel *amqp.Channel
    }
)

func (em *ExchangeManager) DeleteMulti(deleteParams ...*DeleteParams) (err error) {
    for _, params := range deleteParams {
        err = em.Delete(params)
        if err != nil {
            return
        }
    }

    return
}

func (em *ExchangeManager) Delete(deleteParams *DeleteParams) (err error) {
    if em.channel == nil || em.channel.IsClosed() {
        err = fmt.Errorf("unable to delete exchange on closed or empty channel")
        return
    }

    err = em.channel.ExchangeDelete(deleteParams.Name, deleteParams.IfUnused, deleteParams.NoWait)
    if err != nil {
        err = fmt.Errorf("exchange delete error: %w", err)
    }

    return
}

func (em *ExchangeManager) DeclareMulti(declareParams ...*DeclareParams) (err error) {
    for _, params := range declareParams {
        err = em.Declare(params)
        if err != nil {
            return
        }
    }

    return
}

func (em *ExchangeManager) Declare(declareParams *DeclareParams) (err error) {
    if em.channel == nil || em.channel.IsClosed() {
        err = fmt.Errorf("unable to declare exchange on closed or empty channel")
        return
    }

    declareFunc := em.channel.ExchangeDeclare
    if declareParams.Passive {
        declareFunc = em.channel.ExchangeDeclarePassive
    }

    err = declareFunc(
        declareParams.Name,
        declareParams.Kind.String(),
        declareParams.Durable,
        declareParams.AutoDelete,
        declareParams.Internal,
        declareParams.NoWait,
        declareParams.Args,
    )

    if err != nil {
        err = fmt.Errorf("exchange declare error: %w", err)
    }

    return
}

func (em *ExchangeManager) BindMulti(bindParams ...*ExchangeBindParams) (err error) {
    for _, params := range bindParams {
        err = em.Bind(params)
        if err != nil {
            return
        }
    }

    return
}

func (em *ExchangeManager) Bind(bindParams *ExchangeBindParams) (err error) {
    if em.channel == nil || em.channel.IsClosed() {
        return fmt.Errorf("unable to bind exchange on closed or empty channel")
    }

    err = em.channel.ExchangeBind(
        bindParams.Destination,
        bindParams.Key,
        bindParams.Source,
        bindParams.NoWait,
        bindParams.Args,
    )

    if err != nil {
        err = fmt.Errorf("exchange bind error: %w", err)
    }

    return
}

func (em *ExchangeManager) UnbindMulti(unbindParams ...*ExchangeBindParams) (err error) {
    for _, params := range unbindParams {
        err = em.Unbind(params)
        if err != nil {
            return
        }
    }

    return
}

func (em *ExchangeManager) Unbind(bindParams *ExchangeBindParams) (err error) {
    if em.channel == nil || em.channel.IsClosed() {
        return fmt.Errorf("unable to unbind exchange on closed or empty channel")
    }

    err = em.channel.ExchangeUnbind(
        bindParams.Destination,
        bindParams.Key,
        bindParams.Source,
        bindParams.NoWait,
        bindParams.Args,
    )

    if err != nil {
        err = fmt.Errorf("exchange unbind error: %w", err)
    }

    return
}

func NewExchangeManager(channel *amqp.Channel) *ExchangeManager {
    return &ExchangeManager{
        channel: channel,
    }
}
