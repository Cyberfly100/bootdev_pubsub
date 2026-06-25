package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	amqp "github.com/rabbitmq/amqp091-go"
)

func CommandSpam(gs *gamelogic.GameState, words []string, ch *amqp.Channel) error {
	if len(words) < 2 {
		return errors.New("usage: spam <number>")
	}

	number, err := strconv.Atoi(words[1])
	if err != nil {
		return fmt.Errorf("Could not convert number of spams: %v.", words[1])
	}

	for range number {
		log := gamelogic.GetMaliciousLog()
		PublishGameLog(ch, gs.GetUsername(), log)
	}
	return nil
}
