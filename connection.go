package rmq

import (
    "context"
    "fmt"
    amqp "github.com/rabbitmq/amqp091-go"
    "github.com/sirupsen/logrus"
)

type (
    //Connection - wrapped connection struct
    Connection struct {
        // ctx - context, stored in wrapper, will be extending by Publisher and Consumer
        ctx context.Context
        // doneFunc - common function for context cancel
        doneFunc context.CancelFunc
        // cfg - connection params
        cfg *ConnectionCfg
        // constructor - closure, which creates a new amqp.Connection and stores it params in external context
        constructor AmqpConnectionConstructor
        // conn - stored amqp.Connection
        conn *amqp.Connection
    }

    // AmqpConnectionConstructor - wrap method for amqp.Connection creation (cases for amqp.DialConfig or other methods)
    AmqpConnectionConstructor func() (*amqp.Connection, error)
)

// NewDefaultConnection - creates new Connection instance with amqp.Dial method for connection inside
func NewDefaultConnection(ctx context.Context, dsn string, cfg *ConnectionCfg) *Connection {
    return NewConnection(ctx, cfg, func() (*amqp.Connection, error) {
        return amqp.Dial(dsn)
    })
}

// NewConnection - creates a new Connection
func NewConnection(ctx context.Context, cfg *ConnectionCfg, constructor AmqpConnectionConstructor) *Connection {
    mainCtx, done := context.WithCancel(ctx)

    return &Connection{
        ctx:         mainCtx,
        doneFunc:    done,
        constructor: constructor,
        cfg:         cfg,
    }
}

// Conn - connection getter
func (cn *Connection) Conn() *amqp.Connection {
    return cn.conn
}

// IsClosed - wrap for amqp.Connection IsClosed method
func (cn *Connection) IsClosed() bool {
    return cn.conn.IsClosed()
}

// Channel - wrap for amqp.Connection Channel method
func (cn *Connection) Channel() (*amqp.Channel, error) {
    return cn.conn.Channel()
}

// Close - connection close wrapped method
func (cn *Connection) Close() error {
    return cn.conn.Close()
}

// Connect - establish connection with rmq, context need for deadline/timeout stories
func (cn *Connection) Connect(ctx context.Context) error {
    conn, err := cn.connect(ctx, cn.constructor)
    if err != nil {
        return err
    }

    cn.conn = conn
    go cn.background()
    return nil
}

// background - running in single goroutine for listening Done ctx errors and amqp.Connection NotifyClose methods
func (cn *Connection) background() {
    ctxDoneChan := cn.ctx.Done()
    notifyClose := cn.conn.NotifyClose(make(chan *amqp.Error, 1))
    defer close(notifyClose)

    for {
        select {
        case <-ctxDoneChan:
            err := cn.conn.Close()
            if err != nil {
                logrus.Errorf("error while rmq conn closing: %s", err)
            }
            logrus.Errorf("connection to rmq is closed, reason: %s", cn.ctx.Err())
            return
        case err := <-notifyClose:
            logrus.WithError(err).Error("connection to rmq was closed")
            cn.doneFunc()
            return
        }
    }
}

// NotifyClose - wrap for amqp.Connection NotifyClose method
func (cn *Connection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
    return cn.conn.NotifyClose(receiver)
}

// connect - ctx dependent private connect method
func (cn *Connection) connect(ctx context.Context, constructor AmqpConnectionConstructor) (*amqp.Connection, error) {
    retry := 1
    for {
        select {
        case <-ctx.Done():
            return nil, fmt.Errorf("unable to connect to rmq: %s", ctx.Err())
        default:
            var conn *amqp.Connection
            var err error
            conn, err = constructor()

            if err != nil {
                logrus.WithField("err", err).Warningf("cannot establish connection to rmq, retry %d", retry)
                retry += 1
                continue
            }

            return conn, nil
        }
    }
}
