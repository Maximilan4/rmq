package rmq

import (
    "context"
    "errors"
    "fmt"
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/sirupsen/logrus"
    "golang.org/x/sync/errgroup"
)

type (
    // ConsumeParams - wrapped amqp.Channel Consume method`s args
    ConsumeParams struct {
        Queue, Consumer                     string
        AutoAck, Exclusive, NoLocal, NoWait bool
        Args                                amqp.Table
    }
    // Consumer - instance for consuming process
    Consumer struct {
        connection *Connection
        cfg        ConsumerConfig
        ctx        context.Context
        done       context.CancelFunc
    }
)

// NewConsumer - creates new consumer with default params
func NewConsumer(connection *Connection, cfg *ConsumerConfig) *Consumer {
    ctx, done := context.WithCancel(connection.ctx)
    consumer := &Consumer{
        connection: connection,
        cfg:        *cfg,
        ctx:        ctx,
        done:       done,
    }

    if consumer.cfg.WorkersCount == 0 {
        consumer.cfg.WorkersCount = 1
    }

    return consumer
}

//StartWorkersGroup - start group of workers for single queue
func (cnr *Consumer) StartWorkersGroup(params *ConsumeParams, handler MessageHandler) (err error) {
    if cnr.connection.IsClosed() {
        err = errors.New("rmq connection not ready")
        return
    }

    group, ctx := errgroup.WithContext(cnr.ctx)

    for workerNum := 1; workerNum <= cnr.cfg.WorkersCount; workerNum++ {
        workerId := workerNum
        group.Go(func() error {
            workerParams := *params
            if workerParams.Consumer != "" {
                workerParams.Consumer = fmt.Sprintf("%s-%d", workerParams.Consumer, workerId)
            } else {
                workerParams.Consumer = fmt.Sprintf("%s-%d", workerParams.Queue, workerId)
            }

            return cnr.StartWorker(ctx, &workerParams, handler)
        })
    }

    if err = group.Wait(); err != nil {
        return err
    }

    return
}

//StartWorker - starts single consumer worker on a single queue
func (cnr *Consumer) StartWorker(ctx context.Context, params *ConsumeParams, handler MessageHandler) error {
    //channel per every worker
    channel, err := cnr.connection.Channel()
    if err != nil {
        return err
    }

    deliveryChan, err := channel.Consume(
        params.Queue,
        params.Consumer,
        params.AutoAck,
        params.Exclusive,
        params.NoLocal,
        params.NoWait,
        params.Args,
    )

    if err != nil {
        return err
    }

    workerCtx, doneFunc := context.WithCancel(ctx)
    defer doneFunc()

    channelErrors := channel.NotifyClose(make(chan *amqp.Error))
    for {
        select {
        //message receiving
        case msg := <-deliveryChan:
            if cnr.cfg.Synchronous {
                cnr.handleMsg(ctx, &msg, handler)
            } else {
                go cnr.handleMsg(ctx, &msg, handler)
            }
        // listener for amqp.Channel errors
        case notifyErr := <-channelErrors:
            return notifyErr
        // stop worker by context
        case <-workerCtx.Done():
            return workerCtx.Err()
        }
    }
}

// handleMsg just calls handler function with additional logs
func (cnr *Consumer) handleMsg(ctx context.Context, msg *amqp.Delivery, handler MessageHandler) {
    logEntry := logrus.WithFields(logrus.Fields{
        "exchange":     msg.Exchange,
        "routing_key":  msg.RoutingKey,
        "delivery_tag": msg.DeliveryTag,
        "redelivered":  msg.Redelivered,
        "tag":          msg.ConsumerTag,
    })

    if err := handler.Handle(ctx, msg); err != nil {
        logEntry.Errorf("msg handling err: %s", err)
    }
    logEntry.Info("message handled")
}
