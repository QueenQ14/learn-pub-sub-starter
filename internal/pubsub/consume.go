package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
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
			ackType := handler(obj)
			switch ackType {
			case Ack:
				fmt.Println("case Ack")
				delivery.Ack(false)
			case NackRequeue:
				fmt.Println("case NackRequeue")
				delivery.Nack(false, true)
			case NackDiscard:
				fmt.Println("case NackDiscard")
				delivery.Nack(false, false)
			default:
				fmt.Printf("unrecognized ack-type: %v\n", ackType)
				continue
			}
		}
	}()
	return nil
}
