package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Acktype int

const (
	Ack Acktype = iota
	NackRequeue
	NackDiscard
)

// SimpleQueueType is an "enum" type to represent transient vs durable queues.
type SimpleQueueType int

const (
	SimpleQueueTransient SimpleQueueType = iota
	SimpleQueueDurable
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	})
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshallerJSON)
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        buffer.Bytes(),
	})
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) Acktype,
) error {
	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshallerGob)
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveriesChan, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveriesChan {
			val, err := unmarshaller(msg.Body)
			if err != nil {
				// If there's an error decoding, we nack the message and continue to the next one.
				//msg.Nack(false, false)
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}

			ack_type := handler(val)

			switch ack_type {
			case Ack:
				msg.Ack(false)
				// log.Printf("Positive ack for message with routing key %s", msg.RoutingKey)
			case NackRequeue:
				msg.Nack(false, true)
				// log.Printf("Negative ack (requeue) for message with routing key %s", msg.RoutingKey)
			case NackDiscard:
				msg.Nack(false, false)
				// log.Printf("Negative ack (discard) for message with routing key %s", msg.RoutingKey)
			}
		}
	}()

	return nil

}

func unmarshallerGob[T any](body []byte) (T, error) {
	var val T
	buffer := bytes.NewBuffer(body)
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&val)
	return val, err
}

func unmarshallerJSON[T any](body []byte) (T, error) {
	var val T
	err := json.Unmarshal(body, &val)
	return val, err
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	q, err := ch.QueueDeclare(
		queueName,
		queueType == SimpleQueueDurable,
		queueType == SimpleQueueTransient,
		queueType == SimpleQueueTransient,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return ch, q, nil
}
