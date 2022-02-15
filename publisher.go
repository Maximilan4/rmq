package rmq

import (
    "context"
    "errors"
    "github.com/jackc/puddle"
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/sirupsen/logrus"
    "time"
)

//PublishMessage - struct with params from amqp.Channel().Publish(...) method
type PublishMessage struct {
    ExchangeName, RoutingKey string
    Mandatory, Immediate     bool
    Publishing               amqp.Publishing
}

//Publisher - struct of publisher
type Publisher struct {
    connection *Connection
    pool       *puddle.Pool
    cfg        PublisherConfig
    ctx        context.Context
    done       context.CancelFunc
}

// NewPublisher - publisher constructor
func NewPublisher(connection *Connection, cfg *PublisherConfig) *Publisher {
    publisher := &Publisher{
        connection: connection,
    }

    publisher.ctx, publisher.done = context.WithCancel(connection.ctx)
    publisher.cfg = *cfg

    if publisher.cfg.MaxIdleTime == 0 {
        publisher.cfg.MaxIdleTime = time.Second * 30
    }

    if publisher.cfg.MaxChannelsCount == 0 {
        publisher.cfg.MaxChannelsCount = 3
    }

    if publisher.cfg.CleanUpInterval == 0 {
        publisher.cfg.CleanUpInterval = time.Minute
    }

    return publisher
}

//Init - runs background tasks and init a pool first element
func (p *Publisher) Init() error {
    p.pool = puddle.NewPool(p.chanInit, p.chanClose, p.cfg.MaxChannelsCount)
    // init first idle channel
    err := p.pool.CreateResource(p.ctx)
    if err != nil {
        p.pool.Close()
        return err
    }

    go p.background()
    return nil
}

// Pool - channels pool getter
func (p *Publisher) Pool() *puddle.Pool {
    return p.pool
}

// Close - closes active connection and channels pool
func (p *Publisher) Close() {
    p.pool.Close()
    p.done()
}

// Publish - publish a message to exchange
func (p *Publisher) Publish(ctx context.Context, msg *PublishMessage) error {
    if p.connection.IsClosed() {
        return errors.New("connection is not ready")
    }

    resource, err := p.pool.Acquire(ctx)
    if err != nil {
        resource.Destroy()
        return err
    }

    channel := resource.Value().(*amqp.Channel)

    err = channel.Publish(msg.ExchangeName, msg.RoutingKey, msg.Mandatory, msg.Immediate, msg.Publishing)
    if err != nil {
        resource.Destroy()
        return err
    }

    resource.Release()

    return nil
}

// chanClose - channel destruction
func (p *Publisher) chanClose(channel interface{}) {
    if channel == nil {
        return
    }

    err := channel.(*amqp.Channel).Close()
    if err != nil {
        logrus.Errorf("error while rmq channel close: %s", err)
        return
    }
    logrus.Infof("rmq channel closed")
}

// chanInit - channel construction
func (p *Publisher) chanInit(ctx context.Context) (interface{}, error) {
    var channel *amqp.Channel
    var err error
    for {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
            channel, err = p.connection.Channel()
            if err != nil {
                logrus.WithField("err", err).Warningf("unable to init rmq channel")
                continue
            }

            logrus.Infof("rmq channel inited")
            return channel, nil
        }
    }
}

//background - producer background tasks
func (p *Publisher) background() {
    ticker := time.NewTicker(p.cfg.CleanUpInterval)
    errChan := p.connection.NotifyClose(make(chan *amqp.Error))
    var notifyErr *amqp.Error
    for {
        select {
        case notifyErr = <-errChan:
            if notifyErr == nil {
                continue
            }

        case tick := <-ticker.C:
            logrus.Infof("clean up event at %s", tick)
            for _, resource := range p.pool.AcquireAllIdle() {
                if resource.IdleDuration() > p.cfg.MaxIdleTime {
                    resource.Destroy()
                } else {
                    resource.Release()
                }
            }
        case <-p.ctx.Done():
            return
        }
    }

}
