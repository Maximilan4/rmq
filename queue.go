package rmq

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type (
	// QueueBindParams - amqp.Channel().QueueBind(...) params
	QueueBindParams struct {
		Name, Key, Exchange string
		NoWait              bool
		Args                amqp.Table
	}
	// QueueManager - queue manager
	QueueManager struct {
		channel *amqp.Channel
	}
)

// Inspect - amqp.Channel().QueueInspect(...) wrap
func (qs *QueueManager) Inspect(name string) (q amqp.Queue, err error) {
	if qs.channel == nil || qs.channel.IsClosed() {
		err = fmt.Errorf("unable to inspect queue on closed or empty channel")
		return
	}
	q, err = qs.channel.QueueInspect(name)
	if err != nil {
		err = fmt.Errorf("queue inspect error: %w", err)
	}

	return
}

// Purge - amqp.Channel().QueuePurge() wrap
func (qs *QueueManager) Purge(name string, noWait bool) (msgCount int, err error) {
	if qs.channel == nil || qs.channel.IsClosed() {
		err = fmt.Errorf("unable to purge queue on closed or empty channel")
		return
	}

	msgCount, err = qs.channel.QueuePurge(name, noWait)
	if err != nil {
		err = fmt.Errorf("queue purge error: %w", err)
	}

	return
}

// DeleteMulti - deletes for than one queue
func (qs *QueueManager) DeleteMulti(deleteParams ...*DeleteParams) (err error) {
	for _, params := range deleteParams {
		_, err = qs.Delete(params)

		if err != nil {
			return
		}
	}

	return
}

// Delete - deletes queue
func (qs *QueueManager) Delete(params *DeleteParams) (msgCount int, err error) {
	if qs.channel == nil || qs.channel.IsClosed() {
		err = fmt.Errorf("unable to delete queue on closed or empty channel")
		return
	}

	msgCount, err = qs.channel.QueueDelete(params.Name, params.IfUnused, params.IfEmpty, params.NoWait)
	if err != nil {
		err = fmt.Errorf("queue delete error: %w", err)
	}

	return
}

// DeclareMulti - declares more than one queue
func (qs *QueueManager) DeclareMulti(declareParams ...*DeclareParams) (err error) {
	for _, params := range declareParams {
		_, err = qs.Declare(params)

		if err != nil {
			return fmt.Errorf("queue declare err: %w", err)
		}
	}

	return
}

// Declare - declare queue
func (qs *QueueManager) Declare(declareParams *DeclareParams) (q amqp.Queue, err error) {
	if qs.channel == nil || qs.channel.IsClosed() {
		err = fmt.Errorf("unable to declare queue on closed or empty channel")
		return
	}

	declareFunc := qs.channel.QueueDeclare
	if declareParams.Passive {
		declareFunc = qs.channel.QueueDeclarePassive
	}

	q, err = declareFunc(
		declareParams.Name,
		declareParams.Durable,
		declareParams.AutoDelete,
		declareParams.Exclusive,
		declareParams.NoWait,
		declareParams.Args,
	)

	if err != nil {
		err = fmt.Errorf("queue declare err: %w", err)
	}

	return
}

// BindMulti - bind more than one queue
func (qs *QueueManager) BindMulti(bindParams ...*QueueBindParams) (err error) {
	for _, params := range bindParams {
		err = qs.Bind(params)

		if err != nil {
			return
		}
	}

	return
}

// Bind - bind queue
func (qs *QueueManager) Bind(bindParams *QueueBindParams) (err error) {
	if qs.channel == nil || qs.channel.IsClosed() {
		return fmt.Errorf("unable to bind queue on closed or empty channel")
	}

	err = qs.channel.QueueBind(
		bindParams.Name,
		bindParams.Key,
		bindParams.Exchange,
		bindParams.NoWait,
		bindParams.Args,
	)

	if err != nil {
		err = fmt.Errorf("queue bind error: %w", err)
	}

	return
}

// UnbindMulti - unbind more than one queue
func (qs *QueueManager) UnbindMulti(bindParams ...*QueueBindParams) (err error) {
	for _, params := range bindParams {
		err = qs.Unbind(params)

		if err != nil {
			return
		}
	}

	return
}

// Unbind - unbind queue
func (qs *QueueManager) Unbind(bindParams *QueueBindParams) (err error) {
	if qs.channel == nil || qs.channel.IsClosed() {
		return fmt.Errorf("unable to unbind queue on closed or empty channel")
	}

	err = qs.channel.QueueUnbind(
		bindParams.Name,
		bindParams.Key,
		bindParams.Exchange,
		bindParams.Args,
	)

	if err != nil {
		err = fmt.Errorf("queue unbind error: %w", err)
	}

	return
}

// NewQueueManager - QueueManager constructor
func NewQueueManager(channel *amqp.Channel) *QueueManager {
	return &QueueManager{
		channel: channel,
	}
}
