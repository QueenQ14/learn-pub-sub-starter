package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	rabbitConnString := "amqp://guest:guest@localhost:5672/"
	rabbitConn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()
	fmt.Println("Connection with RabbitMQ has been established!")
	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("error welcoming user: %v", err)
	}
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, userName)
	gameState := gamelogic.NewGameState(userName)
	if err := pubsub.SubscribeJSON(
		rabbitConn,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.TransientQueue,
		handlerPause(gameState),
	); err != nil {
		log.Fatal(err)
	}
	movesChannel, err := rabbitConn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	if err := pubsub.SubscribeJSON(
		rabbitConn,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+userName,
		routing.ArmyMovesPrefix+".*",
		pubsub.TransientQueue,
		handlerMove(gameState),
	); err != nil {
		log.Fatal(err)
	}
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "spawn":
			if err := gameState.CommandSpawn(input); err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			err = pubsub.PublishJSON(movesChannel, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+userName, move)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(move.Units), move.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Unknown command. Use 'help' to get a list of commands.")
		}
	}
}
