package utils

import (
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"
)

func SendBatchesToKafka(producer sarama.SyncProducer, topic string, batches [][]string, batchSize int) {
	var wg sync.WaitGroup

	for _, batch := range batches {
		wg.Add(1)
		go func(batch []string) {
			defer wg.Done()
			combinedData := strings.Join(batch, ",") // Assume data is comma-separated
			message := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(combinedData),
			}
			_, _, err := producer.SendMessage(message)
			if err != nil {
				log.Printf("Failed to send message to Kafka: %v", err)
			}
		}(batch)
	}

	wg.Wait()
}
