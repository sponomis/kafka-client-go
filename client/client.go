package client

import "C"
import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/sponomis/confluent-kafka-go/kafka"
	log "github.com/sirupsen/logrus"
	"os"
)

type ConfigProducer struct {
	producer kafka.Producer
	topic    string
	open     bool
	error    bool
}

type ConfigConsumer struct {
	Consumer kafka.Consumer
	Open     bool
	Error    bool
}

func (p *ConfigProducer) Flush() int {

	// Utilizar somente no Close()
	p.producer.Flush(5 * 1000)

	return 0
}

func (p *ConfigProducer) Close() {

	p.Flush()
	p.producer.Close()

}

func (p *ConfigConsumer) CloseConsumer() {

	_ = p.Consumer.Close()

}

func (p *ConfigProducer) SendMessage(data map[string]interface{}) {

	jsonData, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &p.topic, Partition: kafka.PartitionAny},
		Value:          jsonData,
	}, nil)

	if err != nil {
		panic(err)
	}

}

func CreateProducer(topic string, config *kafka.ConfigMap) (*ConfigProducer, error) {

	prod, err := kafka.NewProducer(config)

	if err != nil {
		log.Errorf("Failed to create producer: %s\n", err)
		return &ConfigProducer{}, errors.New("empty name")
	}

	// Wait for delivery report
	go func() {
		for e := range prod.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Warningf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Debugf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	return &ConfigProducer{producer: *prod, topic: topic, open: true}, nil

}

func CreateConsumer(config kafka.ConfigMap) (*ConfigConsumer, error) {

	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		return &ConfigConsumer{}, err
	}

	return &ConfigConsumer{Consumer: *consumer, Open: true}, nil

}
