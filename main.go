package main

import (
	"context"
	"go-kafka/config"
	"go-kafka/consumer"
	"go-kafka/handler"
	"go-kafka/processor"
	"go-kafka/producer"
	"go-kafka/utils"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main2() {
	brokers := []string{"localhost:9092"}
	groupID := "your-group-id"
	topics := []string{"your-topic"}

	conn, err := config.ConnectDB()
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close(context.Background())

	producer := producer.NewKafkaProducer(brokers)
	defer producer.Close()

	batches, err := processor.ProcessAndSplitExcel("yourfile.xlsx", 100)
	if err != nil {
		log.Fatalf("Failed to process Excel file: %v", err)
	}

	utils.SendBatchesToKafka(producer, "your-topic", batches, 100)

	consumerGroup, err := consumer.NewKafkaConsumer2(brokers, groupID, topics)
	if err != nil {
		log.Fatalf("Failed to start Kafka consumer group: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	consumer := handler.ConsumerGroupHandler{conn: conn}

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, topics, consumer); err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}
		}
	}()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm

	if err = consumerGroup.Close(); err != nil {
		log.Fatalf("Error closing consumer group: %v", err)
	}
}
