package presets

import (
    "errors"
    "fmt"
    "github.com/Maximilan4/rmq"
    amqp "github.com/rabbitmq/amqp091-go"
    "strings"
)

type FallbackStrategyPreset struct {
    Queue, DelayQueue, FailedQueue *rmq.DeclareParams
    ExchangeName                   string
    QueueRoutingKey                string
}

func (fsp *FallbackStrategyPreset) Apply(_ *amqp.Channel, schema *rmq.Schema) (err error) {
    if fsp.Queue == nil {
        err = errors.New("main queue declare params is not set")
        return
    }

    if fsp.DelayQueue == nil {
        fsp.DelayQueue = fsp.getQueueParamsCopy("delay")
    }

    if fsp.FailedQueue == nil {
        fsp.FailedQueue = fsp.getQueueParamsCopy("failed")
    }

    failedRk := strings.Join([]string{fsp.QueueRoutingKey, "failed"}, ".")
    delayRk := strings.Join([]string{fsp.QueueRoutingKey, "delay"}, ".")

    fsp.setDeadLetterParams(fsp.Queue, fsp.ExchangeName, failedRk)
    fsp.setDeadLetterParams(fsp.DelayQueue, fsp.ExchangeName, fsp.QueueRoutingKey)

    err = schema.Queue.DeclareMulti(fsp.Queue, fsp.DelayQueue, fsp.FailedQueue)
    if err != nil {
        err = fmt.Errorf("FallbackStrategyPreset err: %w", err)
        return
    }

    err = schema.Queue.BindMulti(
        &rmq.QueueBindParams{
            Name:     fsp.Queue.Name,
            Key:      fsp.QueueRoutingKey,
            Exchange: fsp.ExchangeName,
        },
        &rmq.QueueBindParams{
            Name:     fsp.DelayQueue.Name,
            Key:      delayRk,
            Exchange: fsp.ExchangeName,
        },
        &rmq.QueueBindParams{
            Name:     fsp.FailedQueue.Name,
            Key:      failedRk,
            Exchange: fsp.ExchangeName,
        },
    )

    if err != nil {
        err = fmt.Errorf("FallbackStrategyPreset err: %w", err)
    }

    return
}

func (fsp *FallbackStrategyPreset) setDeadLetterParams(declareParams *rmq.DeclareParams, exchange, rk string) {
    if declareParams.Args == nil {
        declareParams.Args = make(amqp.Table)
    }

    if _, ok := declareParams.Args["x-dead-letter-exchange"]; !ok {
        declareParams.WithDeadLetterExchange(exchange)
    }

    if _, ok := declareParams.Args["x-dead-letter-routing-key"]; !ok {
        declareParams.WithDeadLetterRk(rk)
    }
}

func (fsp *FallbackStrategyPreset) getQueueParamsCopy(postfix string) *rmq.DeclareParams {
    params := *fsp.Queue
    params.Name = fmt.Sprintf("%s_%s", params.Name, postfix)
    if fsp.Queue != nil {
        argsCopy := make(amqp.Table)
        for k, v := range fsp.Queue.Args {
            argsCopy[k] = v
        }
        params.Args = argsCopy
    }
    return &params
}

func NewFallbackStrategyPreset(exchangeName, rk string, queue, delayQueue, failedQueue *rmq.DeclareParams) *FallbackStrategyPreset {
    return &FallbackStrategyPreset{
        Queue:           queue,
        DelayQueue:      delayQueue,
        FailedQueue:     failedQueue,
        ExchangeName:    exchangeName,
        QueueRoutingKey: rk,
    }
}
