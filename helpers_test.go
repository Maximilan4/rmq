package rmq

import (
	"bytes"
	amqp "github.com/rabbitmq/amqp091-go"
	"reflect"
	"testing"
	"time"
)

func Test_durationToExpiration(t *testing.T) {
	type args struct {
		duration time.Duration
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"Less than millisecond",
			args{duration: time.Duration(10)},
			"0",
		},
		{
			"Zero value",
			args{duration: time.Duration(0)},
			"0",
		},
		{
			"Minute",
			args{duration: time.Minute},
			"60000",
		},
		{
			"Not clear mod 1000",
			args{duration: 65536344334 * time.Nanosecond},
			"65536",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := durationToExpiration(tt.args.duration); got != tt.want {
				t.Errorf("durationToExpiration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_createPublishingFromDelivery(t *testing.T) {
	delivery := &amqp.Delivery{
		Headers: amqp.Table{
			"x-header": "test_header_value",
		},
		ContentType:     "text/plain",
		ContentEncoding: "gzip",
		Body:            []byte("test body"),
	}

	publishing := createPublishingFromDelivery(delivery)
	if !reflect.DeepEqual(delivery.Headers, publishing.Headers) {
		t.Errorf("headers mismatch, expected %s, got %s", delivery.Headers, publishing.Headers)
	}

	if publishing.ContentType != delivery.ContentType {
		t.Errorf("content type mismatch, expected %s, got %s", delivery.ContentType, publishing.ContentType)
	}

	if publishing.ContentEncoding != delivery.ContentEncoding {
		t.Errorf("content encoding mismatch, expected %s, got %s", delivery.ContentEncoding, publishing.ContentEncoding)
	}

	if !bytes.Equal(delivery.Body, publishing.Body) {
		t.Errorf("content body mismatch, expected %s, got %s", delivery.Body, publishing.Body)
	}
}
