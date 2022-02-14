package rmq

import (
    "context"
    "errors"
    "fmt"
    amqp "github.com/rabbitmq/amqp091-go"
    "time"
)

type FallbackMessageHandler struct {
    *DefaultMessageHandler
    DelayRoutingKey, DelayExchangeName string
    DelayMs                            time.Duration
    RetriesCount                       int64
}

//NewFallbackMessageHandler - DefaultMessageHandler constructor with custom handle func as required value
func NewFallbackMessageHandler(
    delayExchangeName, delayRk string,
    delay time.Duration,
    retriesCount int64,
    handleFunc HandleFunc,
) *FallbackMessageHandler {
    return &FallbackMessageHandler{
        DefaultMessageHandler: &DefaultMessageHandler{HandleFunc: handleFunc},
        DelayRoutingKey:       delayRk,
        DelayExchangeName:     delayExchangeName,
        DelayMs:               delay,
        RetriesCount:          retriesCount,
    }
}

func (fmh *FallbackMessageHandler) Handle(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) (err error) {
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

        if retriesCount < fmh.RetriesCount {
            newMsg := createPublishingFromDelivery(msg)
            newMsg.Expiration = durationToExpiration(fmh.DelayMs)
            pErr = channel.Publish(
                fmh.DelayExchangeName,
                fmh.DelayRoutingKey,
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
