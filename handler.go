package rmq

import (
    "context"
    "errors"
    "fmt"
    amqp "github.com/rabbitmq/amqp091-go"
)

// MsgAction - type for declaring action after msg handle (ack, nack, etc)
type MsgAction int

const (
    //ActionAck - acknowledge message
    ActionAck MsgAction = 1 + iota
    //ActionNack - not acknowledge without requeue
    ActionNack
    //ActionRequeue - not acknowledge with requeue
    ActionRequeue
    //ActionReject - reject message
    ActionReject
)

type (
    // BeforeHandleFunc - see DefaultMessageHandler for usage
    BeforeHandleFunc func(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) error
    // HandleFunc - see DefaultMessageHandler for usage
    HandleFunc func(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) (MsgAction, error)
    // AfterHandleFunc - see DefaultMessageHandler for usage
    AfterHandleFunc func(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery, action MsgAction) error
    //MessageHandler - Base message handler interface
    MessageHandler interface {
        Handle(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) error
    }
    //DefaultMessageHandler - default message handler
    DefaultMessageHandler struct {
        // BeforeHandle - custom before handle func
        BeforeHandleFunc BeforeHandleFunc
        // HandleFunc - custom handle func
        HandleFunc HandleFunc
        // AfterHandleFunc - custom after handle func
        AfterHandleFunc AfterHandleFunc
    }
)

//BeforeHandle - if BeforeHandleFunc specified -> runs it
func (dmh *DefaultMessageHandler) BeforeHandle(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) error {
    if dmh.BeforeHandleFunc != nil {
        return dmh.BeforeHandleFunc(ctx, channel, msg)
    }

    return nil
}

//Handle - main handle function, wraps result of HandleFunc to MessageResultContainer
//if HandleFunc is nil -> returns nil, msg will be nacked by AfterHandle event
func (dmh *DefaultMessageHandler) Handle(ctx context.Context, channel *amqp.Channel, msg *amqp.Delivery) (err error) {
    if err = dmh.BeforeHandle(ctx, channel, msg); err != nil {
        return
    }

    if dmh.HandleFunc == nil {
        err = errors.New("HandleFunc is required for default message handler")
        return
    }

    action, err := dmh.HandleFunc(ctx, channel, msg)
    if err != nil {
        err = fmt.Errorf("msg handling error: %w", err)
        aErr := dmh.DoMsgAction(msg, ActionRequeue)
        if aErr != nil {
            err = fmt.Errorf("requeue error: %s, prev err: %w", aErr.Error(), err)
        }

        return
    }

    if err = dmh.AfterHandle(ctx, msg, channel, action); err != nil {
        return
    }

    return
}

//AfterHandle - if AfterHandleFunc specified -> runs it, else check HandleResult for errors and ack or nack msg
func (dmh *DefaultMessageHandler) AfterHandle(
    ctx context.Context,
    msg *amqp.Delivery,
    channel *amqp.Channel,
    action MsgAction,
) error {
    if dmh.AfterHandleFunc != nil {
        return dmh.AfterHandleFunc(ctx, channel, msg, action)
    }

    err := dmh.DoMsgAction(msg, action)
    if err != nil {
        return fmt.Errorf("error while broker notify action: %w", err)
    }

    return nil
}

//DoMsgAction - do ack or nack job with readed message
func (dmh *DefaultMessageHandler) DoMsgAction(msg *amqp.Delivery, action MsgAction) (err error) {
    switch action {
    case ActionAck:
        err = msg.Ack(false)
    case ActionNack:
        err = msg.Nack(false, false)
    case ActionRequeue:
        err = msg.Nack(false, true)
    case ActionReject:
        err = msg.Reject(false)
    default:
        err = fmt.Errorf("wrong action received: %d", action)
    }

    return
}

//NewDefaultMessageHandler - DefaultMessageHandler constructor with custom handle func as required value
func NewDefaultMessageHandler(handleFunc HandleFunc) *DefaultMessageHandler {
    return &DefaultMessageHandler{HandleFunc: handleFunc}
}
