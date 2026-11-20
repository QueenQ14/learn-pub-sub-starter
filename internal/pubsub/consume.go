package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T)) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("error binding queue: %w", err)
	}
	deliveryChan, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %w", err)
	}
	go func() {
		defer channel.Close()
		for delivery := range deliveryChan {
			var obj T
			if err := json.Unmarshal(delivery.Body, &obj); err != nil {
				fmt.Printf("error unmarshalling delivery body: %v\n", err)
				continue
			}
			handler(obj)
			delivery.Ack(false)
		}
	}()
	return nil
}
