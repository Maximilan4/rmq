package rmq

import (
    "context"
    "errors"
    "fmt"
    amqp "github.com/rabbitmq/amqp091-go"
    "time"
)

//DelayedRetryMessageHandler - extended DefaultMessageHandler with delay logic
type DelayedRetryMessageHandler struct {
    *DefaultMessageHandler
    //DelayQueueRoutingKey - queue routing key for delayed messages resend
    DelayQueueRoutingKey string
    // DelayExchangeName - exchange name for delayed messages resend
    DelayExchangeName string
    // Delay - time interval for message expiration in delay queue
    Delay time.Duration
    // MaxRetriesCount - maximum count of retires before message will be rejected
    MaxRetriesCount int64
}

//NewDelayedRetryMessageHandler - DefaultMessageHandler constructor with custom handle func as required value
func NewDelayedRetryMessageHandler(
    delayExchangeName, delayRk string,
    delay time.Duration,
    retriesCount int64,
    handleFunc HandleFunc,
) *DelayedRetryMessageHandler {
    return &DelayedRetryMessageHandler{
        DefaultMessageHandler: &DefaultMessageHandler{HandleFunc: handleFunc},
        DelayQueueRoutingKey:  delayRk,
        DelayExchangeName:     delayExchangeName,
        Delay:                 delay,
        MaxRetriesCount:       retriesCount,
    }
}

//Handle - redeclared DefaultMessageHandler.Handle method, main difference in error handling logic
// if HandleFunc returns err, by default message will be rejected
// but if message has x-death header and count < MaxRetriesCount -> message will be acknowledged and resented to delay queue
func (fmh *DelayedRetryMessageHandler) Handle(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) (err error) {
    if err = fmh.BeforeHandle(ctx, channel, msg); err != nil {
        return
    }

    if fmh.HandleFunc == nil {
        err = errors.New("HandleFunc is required for default message handler")
        return
    }

    action, err := fmh.HandleFunc(ctx, channel, msg)
    if err != nil {
        err = fmt.Errorf("msg handling error: %w", err)
        var aErr, pErr error
        fallbackAction := ActionReject

        retriesCount := getExpiredMsgRetriesCount(msg)

        if retriesCount < fmh.MaxRetriesCount {
            newMsg := createPublishingFromDelivery(msg)
            newMsg.Expiration = durationToExpiration(fmh.Delay)
            pErr = channel.Publish(
                fmh.DelayExchangeName,
                fmh.DelayQueueRoutingKey,
                false,
                false,
                newMsg,
            )
            if pErr != nil {
                err = fmt.Errorf("publishing to delay queue err: %s, prev err : %w", pErr, err)
            } else {
                fallbackAction = ActionAck
            }
        }

        aErr = fmh.DoMsgAction(msg, fallbackAction)

        if aErr != nil {
            err = fmt.Errorf("%s, prev err: %w", aErr, err)
        }

        return
    }

    if err = fmh.AfterHandle(ctx, msg, channel, action); err != nil {
        return
    }

    return
}
