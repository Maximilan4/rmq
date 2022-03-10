package presets

import (
	"errors"
	"fmt"
	"github.com/Maximilan4/rmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
)

// DelayedRetryStrategyPreset - creates a queue set for delayed msg handling retry
type (
	DelayedRetryStrategyPreset struct {
		// Queue - main queue declare params, is required
		Queue *rmq.DeclareParams
		// DelayQueue - queue for storing messages, which will be sent to main q after expiration, not required
		DelayQueue *rmq.DeclareParams
		// FailedQueue - queue for storing messages, which has an error at handling after n retries
		FailedQueue *rmq.DeclareParams
		// ExchangeName - name of core exchange, which will has binds to all 3 queues
		ExchangeName string
		// RoutingKeys - main rk for bind, binds to delay and failed queues will be generated from this value
		RoutingKeys RoutingKeys
	}
	RoutingKeys struct {
		MainQueueRK    string
		DelayedQueueRK string
		FailedQueueRK  string
	}
)

// Apply - applies preset
func (drsp *DelayedRetryStrategyPreset) Apply(_ *amqp.Channel, schema *rmq.Schema) (err error) {
	if drsp.Queue == nil {
		err = errors.New("main queue declare params is not set")
		return
	}

	if drsp.DelayQueue == nil {
		drsp.DelayQueue = drsp.getQueueParamsCopy("delay")
	}

	if drsp.FailedQueue == nil {
		drsp.FailedQueue = drsp.getQueueParamsCopy("failed")
	}

	if drsp.RoutingKeys.MainQueueRK == "" {
		drsp.RoutingKeys.MainQueueRK = drsp.Queue.Name
	}

	if drsp.RoutingKeys.FailedQueueRK == "" {
		drsp.RoutingKeys.FailedQueueRK = strings.Join([]string{drsp.RoutingKeys.MainQueueRK, "failed"}, ".")
	}

	if drsp.RoutingKeys.DelayedQueueRK == "" {
		drsp.RoutingKeys.DelayedQueueRK = strings.Join([]string{drsp.RoutingKeys.MainQueueRK, "delay"}, ".")
	}

	drsp.setDeadLetterParams(drsp.Queue, drsp.ExchangeName, drsp.RoutingKeys.FailedQueueRK)
	drsp.setDeadLetterParams(drsp.DelayQueue, drsp.ExchangeName, drsp.RoutingKeys.MainQueueRK)

	err = schema.Queue.DeclareMulti(drsp.Queue, drsp.DelayQueue, drsp.FailedQueue)
	if err != nil {
		err = fmt.Errorf("DelayedRetryStrategyPreset err: %w", err)
		return
	}

	err = schema.Queue.BindMulti(
		&rmq.QueueBindParams{
			Name:     drsp.Queue.Name,
			Key:      drsp.RoutingKeys.MainQueueRK,
			Exchange: drsp.ExchangeName,
		},
		&rmq.QueueBindParams{
			Name:     drsp.DelayQueue.Name,
			Key:      drsp.RoutingKeys.DelayedQueueRK,
			Exchange: drsp.ExchangeName,
		},
		&rmq.QueueBindParams{
			Name:     drsp.FailedQueue.Name,
			Key:      drsp.RoutingKeys.FailedQueueRK,
			Exchange: drsp.ExchangeName,
		},
	)

	if err != nil {
		err = fmt.Errorf("DelayedRetryStrategyPreset err: %w", err)
	}

	return
}

// setDeadLetterParams - sets to declareParams x-dead-letter-* headers
func (drsp *DelayedRetryStrategyPreset) setDeadLetterParams(declareParams *rmq.DeclareParams, exchange, rk string) {
	if declareParams.Args == nil {
		declareParams.Args = make(amqp.Table)
	}

	if _, ok := declareParams.Args["x-dead-letter-exchange"]; !ok {
		declareParams.WithDeadLetterExchange(exchange)
	}

	if _, ok := declareParams.Args["x-dead-letter-routing-key"]; !ok {
		declareParams.WithDeadLetterRk(rk)
	}
}

// getQueueParamsCopy - get a copy of DelayedRetryStrategyPreset.Queue with another name with postfix
func (drsp *DelayedRetryStrategyPreset) getQueueParamsCopy(postfix string) *rmq.DeclareParams {
	params := *drsp.Queue
	params.Name = fmt.Sprintf("%s_%s", params.Name, postfix)
	if drsp.Queue != nil {
		argsCopy := make(amqp.Table)
		for k, v := range drsp.Queue.Args {
			argsCopy[k] = v
		}
		params.Args = argsCopy
	}
	return &params
}
