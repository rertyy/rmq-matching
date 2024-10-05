package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

// Connection and channel should be shared across the application
var conn *amqp.Connection
var ch *amqp.Channel
var q amqp.Queue

var topicQueue = make(map[string]map[int][]*User) // map[topic]map[difficulty]User
var queuingUsers = make(map[string][]*User)

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

func consumeQueue() {
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	go func() {
		for msg := range msgs {
			user := DeserialiseUser(string(msg.Body))
			fmt.Println("Received a message: ", user)
			handleMatching(&user)
			// I think it should be fine to ack here rather than after matching
			msg.Ack(false)

			fmt.Println(topicQueue)
			fmt.Println(queuingUsers)
		}

	}()
	fmt.Println("Waiting for messages...")

	select {} // Block forever
}

func handleMatching(user *User) {
	if !user.New {
		removeUserFromQueue(user.ID)
	}

	if _, ok := queuingUsers[user.ID]; ok {
		// Reset all previous queue entries for this user
		removeUserFromQueue(user.ID)
	}
	addUserToQueue(user)
}

func initiateCollaboration(user1, user2 *User) {
	fmt.Printf("Starting collaboration between %s and %s\n", user1, user2)
}

func addUserToQueue(user *User) {
	if queuingUsers[user.ID] == nil {
		queuingUsers[user.ID] = make([]*User, 0)
	}
	queuingUsers[user.ID] = append(queuingUsers[user.ID], user)

	for _, topic := range user.Topic {
		for _, difficulty := range user.Difficulty {
			if topicQueue[topic] == nil {
				topicQueue[topic] = make(map[int][]*User)
			}

			if len(topicQueue[topic][difficulty]) > 0 {

				otherUser := topicQueue[topic][difficulty][0]
				topicQueue[topic][difficulty] = topicQueue[topic][difficulty][1:]

				initiateCollaboration(user, otherUser)
				removeUserFromQueue(otherUser.ID)
				removeUserFromQueue(user.ID)
				return
			}

			topicQueue[topic][difficulty] = append(topicQueue[topic][difficulty], user)

		}
	}
}

func removeUserFromQueue(userId string) {
	fmt.Println("Removing user from queue: ", userId)
	inQueue := queuingUsers[userId]
	delete(queuingUsers, userId)

	for _, u := range inQueue {
		u.New = false
	}
}

func main() {
	initRabbitMQ()
	defer conn.Close()
	defer ch.Close()

	consumeQueue() // Start consuming the queue
}
