package handler

import (
	"go-kafka/config"
	"log"
	"strings"

	"github.com/IBM/sarama"
	"github.com/jackc/pgx/v4"
)

type ConsumerGroupHandler struct {
	conn *pgx.Conn
}

func (h ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h ConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		data := string(message.Value)
		rows := strings.Split(data, ",") // Assuming rows are comma-separated
		for _, row := range rows {
			rowData := strings.Split(row, ",")
			if err := config.SaveToDatabase(h.conn, rowData); err != nil {
				log.Printf("Failed to save to database: %v", err)
			}
		}
		sess.MarkMessage(message, "")
	}
	return nil
}
