package rmq

import (
    "context"
    "errors"
    "fmt"
    "github.com/sirupsen/logrus"
    "github.com/streadway/amqp"
    "golang.org/x/sync/errgroup"
    "os"
    "os/signal"
    "syscall"
)

type ConsumeParams struct {
    Queue, Consumer                     string
    AutoAck, Exclusive, NoLocal, NoWait bool
    Args                                amqp.Table
}

type Consumer struct {
    *Connector
    cfg   ConsumerConfig
    ctx   context.Context
    done  context.CancelFunc
}

// NewConsumer - creates new consumer with default params
func NewConsumer(ctx context.Context, cfg *ConsumerConfig) *Consumer {
    ctx, done := context.WithCancel(ctx)
    consumer := &Consumer{Connector: &Connector{}, cfg: *cfg, ctx: ctx, done: done}
    if consumer.cfg.WorkersCount == 0 {
        consumer.cfg.WorkersCount = 1
    }

    return consumer
}

//Connect - establishing main connection
func (c *Consumer) Connect(ctx context.Context) error {
    err := c.Connector.Connect(ctx, c.cfg.Dsn)
    if err != nil {
        return err
    }
    go c.background()

    return nil
}

// background - handling background tasks
func (c *Consumer) background() {
    osSignal := make(chan os.Signal, 1)
    connErrChan := c.NotifyClose(make(chan *amqp.Error))
    signal.Notify(osSignal, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

    for {
        select {
        //listens for connection errors
        case notifyErr := <- connErrChan:
            c.done()
            logrus.Errorf("broken connection: %s", notifyErr)
            continue
        // handling os signal
        case sig := <-osSignal:
            logrus.Infof("closing connection by system signal: %s", sig.String())
            c.done()
            c.conn.Close()
        case <-c.ctx.Done():
            logrus.Error(c.ctx.Err())
            return
        }
    }
}

//StartWorkersGroup - start group of workers
func (c *Consumer) StartWorkersGroup(params *ConsumeParams, handler MessageHandler) (err error) {
    if c.conn.IsClosed() {
        err = errors.New("rmq connection not ready")
        return
    }

    group, ctx := errgroup.WithContext(c.ctx)

    for workerNum := 1; workerNum <= c.cfg.WorkersCount; workerNum++ {
        workerId := workerNum
        group.Go(func() error {
            workerParams := *params
            if workerParams.Consumer != "" {
                workerParams.Consumer = fmt.Sprintf("%s-%d", workerParams.Consumer, workerId)
            } else {
                workerParams.Consumer = fmt.Sprintf("%s-%d", workerParams.Queue, workerId)
            }

            return c.Worker(ctx, &workerParams, handler)
        })
    }

    if err = group.Wait(); err != nil {
        return err
    }

    return
}

//Worker - main worker func
func (c *Consumer) Worker(ctx context.Context, params *ConsumeParams, handler MessageHandler) error {
    //init channel per single worker
    channel, err := c.conn.Channel()
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

    channelErrors := channel.NotifyClose(make(chan *amqp.Error))
    for {
        select {
        //message receiving
        case msg := <- deliveryChan:
            if c.cfg.Synchronous {
                c.handleMsg(ctx, &msg, handler)
            } else {
                go c.handleMsg(ctx, &msg, handler)
            }
        // listener for amqp.Channel errors
        case notifyErr := <-channelErrors:
            return notifyErr
        // stop worker by context
        case <-ctx.Done():
            return ctx.Err()
        }
    }
}

func (c *Consumer) handleMsg(ctx context.Context, msg *amqp.Delivery, handler MessageHandler) {
    logEntry := logrus.WithFields(logrus.Fields{
        "exchange": msg.Exchange,
        "routing_key": msg.RoutingKey,
        "delivery_tag": msg.DeliveryTag,
        "redelivered": msg.Redelivered,
        "tag": msg.ConsumerTag,
    })

    logEntry.Info("message received")

    if err := handler.Handle(ctx, msg); err != nil {
        logEntry.Errorf("msg handling err: %s", err)
    }
    logEntry.Info("message handled")

}