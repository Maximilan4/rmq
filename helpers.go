package rmq

import (
    amqp "github.com/rabbitmq/amqp091-go"
    "strconv"
    "time"
)

func durationToExpiration(duration time.Duration) string {
    return strconv.FormatInt(int64(duration/time.Millisecond), 10)
}

func createPublishingFromDelivery(delivery *amqp.Delivery) amqp.Publishing {
    return amqp.Publishing{
        Headers:         delivery.Headers,
        ContentType:     delivery.ContentType,
        ContentEncoding: delivery.ContentEncoding,
        Body:            delivery.Body,
    }
}

func getExpiredMsgRetriesCount(msg *amqp.Delivery) (retryCount int64) {
    if _, exists := msg.Headers["x-death"]; !exists {
        return
    }

    xDeath := msg.Headers["x-death"].([]interface{})
    for _, event := range xDeath {
        event := event.(amqp.Table)
        if reason, ok := event["reason"]; !ok || reason != "expired" {
            continue
        }
        if count, ok := event["count"]; ok {
            retryCount = count.(int64)
            return
        }
    }

    return
}
