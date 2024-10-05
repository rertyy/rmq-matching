package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// Connection and channel should be shared across the application
var conn *amqp.Connection
var ch *amqp.Channel
var q amqp.Queue

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func initRabbitMQ() {
	var err error

	conn, err = amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel")

	q, err = ch.QueueDeclare(
		"match_queue", // name of the queue
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		nil,           // arguments
	)
	failOnError(err, "Failed to declare a queue")
}

// Producer function: pushes a message to the RabbitMQ queue
func enqueueUser(userID string, topic []string, difficulty []int, new bool) {
	newUser := &User{ID: userID, Topic: topic, Difficulty: difficulty, New: new}
	message := SerialiseUser(*newUser)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := ch.PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key (queue name)
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(message),
		},
	)
	failOnError(err, "Failed to publish a message")
	fmt.Println("User queued for matching: ", newUser)

}

func main() {
	initRabbitMQ()
	defer conn.Close()
	defer ch.Close()

	// Simulate enqueueing users
	enqueueUser("user1", []string{"Go"}, []int{3}, true)
	enqueueUser("user2", []string{"Python"}, []int{2}, true)
	enqueueUser("user3", []string{"Go"}, []int{1}, true)
	enqueueUser("user4", []string{"Go"}, []int{2}, true)
	enqueueUser("user5", []string{"Go"}, []int{3}, true)
}
