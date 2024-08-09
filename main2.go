package main

import (
	"database/sql"
	"fmt"
	"go-kafka/config"
	"go-kafka/consumer"
	"go-kafka/processor"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	brokers := []string{"localhost:9092"}
	producer := config.NewKafkaProducer(brokers)
	defer producer.Close()

	db, err := sql.Open("postgres", "user=postgres password=hadimas dbname=kafka-explore sslmode=disable")
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	fmt.Println(`[main2.go] [main] -> Connected to PostgreSQL Database`)
	defer db.Close()

	consumer.NewKafkaConsumer(brokers, "your_group_id", "your_topic", db)

	r.POST("/upload", func(c *gin.Context) {
		file, _ := c.FormFile("file")
		filePath := "./" + file.Filename
		if err := c.SaveUploadedFile(file, filePath); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save file"})
			return
		}
		go processor.ProcessExcelFile(filePath, producer, "your_topic")
		c.JSON(http.StatusOK, gin.H{"status": "File uploaded successfully"})
	})

	if err := r.Run(":9090"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}
