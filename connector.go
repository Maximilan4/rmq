package rmq

import (
    "context"
    "fmt"
    "github.com/sirupsen/logrus"
    "github.com/streadway/amqp"
)

//Connector - inner connection struct
type Connector struct {
    conn *amqp.Connection
}

// Conn - connection getter
func (c *Connector) Conn() *amqp.Connection {
    return c.conn
}

// SetConn - for setting
func (c *Connector) SetConn(conn *amqp.Connection) {
    c.conn = conn
}

// Connect - establish connection with rmq
func (c *Connector) Connect(ctx context.Context, dsn string) error {
    retry := 1
    for {
        select {
        case <-ctx.Done():
            return fmt.Errorf("unable to Connect to rmq: %s", ctx.Err())
        default:
            conn, err := amqp.Dial(dsn)
            if err != nil {
                logrus.WithField("err", err).Warningf("cannot establish connection to rmq, retry %d", retry)
                retry += 1
                continue
            }

            logrus.Infof("rmq connection inited")
            c.conn = conn
            return nil
        }
    }
}

func (c *Connector) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
    return c.conn.NotifyClose(receiver)
}
