package rmq

import (
    "context"
    "errors"
    "fmt"
    "github.com/streadway/amqp"
)

type MsgAction int

const (
    ActionAck MsgAction = 1 + iota
    ActionNack
    ActionRequeue
)

type BeforeHandleFunc func (ctx context.Context, msg *amqp.Delivery) error
type HandleFunc func (ctx context.Context, msg *amqp.Delivery) (MsgAction, error)
type AfterHandleFunc func (ctx context.Context, msg *amqp.Delivery, action MsgAction) error


//MessageHandler - Base message handler interface
type MessageHandler interface {
    Handle(ctx context.Context, msg *amqp.Delivery) error
}

//DefaultMessageHandler - default message handler
type DefaultMessageHandler struct {
    // BeforeHandle - custom before handle func
    BeforeHandleFunc BeforeHandleFunc
    // HandleFunc - custom handle func
    HandleFunc HandleFunc
    // AfterHandleFunc - custom after handle func
    AfterHandleFunc AfterHandleFunc
}

//NewDefaultMessageHandler - DefaultMessageHandler constructor with custom handle func as required value
func NewDefaultMessageHandler(handleFunc HandleFunc) *DefaultMessageHandler {
    return &DefaultMessageHandler{HandleFunc: handleFunc}
}

//BeforeHandle - if BeforeHandleFunc specified -> runs it, else just logging incoming message
func (dmh *DefaultMessageHandler) BeforeHandle(ctx context.Context, msg *amqp.Delivery) error {
    if dmh.BeforeHandleFunc != nil {
        return dmh.BeforeHandleFunc(ctx, msg)
    }

    return nil
}

//Handle - main handle function, wraps result of HandleFunc to MessageResultContainer
//if HandleFunc is nil -> returns nil, msg will be nacked by AfterHandle event
func (dmh *DefaultMessageHandler) Handle(ctx context.Context, msg *amqp.Delivery) (err error) {
    if err = dmh.BeforeHandle(ctx, msg); err != nil {
        return
    }

    if dmh.HandleFunc == nil {
        return errors.New("HandleFunc is required for default message handler")
    }

    action, err := dmh.HandleFunc(ctx, msg)
    if err != nil {
        aErr := dmh.DoMsgAction(msg, ActionRequeue)
        if aErr != nil {
            return fmt.Errorf("error while msg handling: %s, requeue failed: %s", err.Error(), aErr.Error())
        }

        return fmt.Errorf("error while msg handling: %s, requeue", err.Error())
    }

    if err = dmh.AfterHandle(ctx, msg, action); err != nil {
        return err
    }

    return
}

//AfterHandle - if AfterHandleFunc specified -> runs it, else check HandleResult for errors and ack or nack msg
func (dmh *DefaultMessageHandler) AfterHandle(ctx context.Context, msg *amqp.Delivery, action MsgAction) error {
    if dmh.AfterHandleFunc != nil {
        return dmh.AfterHandleFunc(ctx, msg, action)
    }

    err := dmh.DoMsgAction(msg, action)
    if err != nil {
        return fmt.Errorf("error while broker notify action: %s", err)
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
    default:
        err = fmt.Errorf("wrong action received: %d", action)
    }

    return
}





