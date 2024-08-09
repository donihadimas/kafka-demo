package consumer

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/lib/pq"
)

func NewKafkaConsumer(brokers []string, groupID, topic string, db *sql.DB) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Failed to create consumer group: %v", err)
	}
	fmt.Println(`[kafka_consumer.go] [NewKafkaConsumer] ->  Sarama Consumer Group Successfully Created`)

	ctx := context.Background()

	go func() {
		for {
			err := consumerGroup.Consume(ctx, []string{topic}, &Consumer{
				db: db,
			})
			if err != nil {
				log.Fatalf("Error from consumer: %v", err)
			}
		}
	}()
}

type Consumer struct {
	db *sql.DB
}

func (c *Consumer) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		c.saveToDB(string(message.Value))
		sess.MarkMessage(message, "")
	}
	return nil
}

func (c *Consumer) saveToDB(data string) {
	rows := strings.Split(data, ",")
	row_num := rows[0]
	product := rows[1]
	stock := rows[2]
	price := rows[3]
	createdOn := time.Now().UTC()
	_, err := c.db.Exec("INSERT INTO product (row_number, product, stock, price, created_on) VALUES ($1, $2, $3 , $4, $5)", row_num, product, stock, price, createdOn)
	if err != nil {
		log.Printf("Failed to insert data into database: %v", err)
	}
}
