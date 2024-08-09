package processor

import (
	"log"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/xuri/excelize/v2"
)

func ProcessExcelFile(filePath string, producer sarama.SyncProducer, topic string) {
	f, err := excelize.OpenFile(filePath)
	if err != nil {
		log.Fatalf("Failed to open Excel file: %v", err)
	}

	rows, err := f.GetRows("Sheet1")
	if err != nil {
		log.Fatalf("Failed to get rows: %v", err)
	}

	var wg sync.WaitGroup
	batchSize := 100
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		wg.Add(1)
		go func(batch [][]string) {
			defer wg.Done()
			for _, row := range batch {
				message := &sarama.ProducerMessage{
					Topic: topic,
					Value: sarama.StringEncoder(rowToString(row)),
				}
				_, _, err := producer.SendMessage(message)
				if err != nil {
					log.Printf("Failed to send message: %v", err)
				}
			}
		}(rows[i:end])
	}
	wg.Wait()
}

// func ProcessAndSplitExcel(filePath string, batchSize int) ([][]string, error) {
// 	f, err := excelize.OpenFile(filePath)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer f.Close()

// 	rows, err := f.GetRows("Sheet1")
// 	if err != nil {
// 		return nil, err
// 	}

// 	var batches [][]string
// 	for i := 0; i < len(rows); i += batchSize {
// 		end := i + batchSize
// 		if end > len(rows) {
// 			end = len(rows)
// 		}
// 		batch := rows[i:end]
// 		for _, row := range batch {
// 			batches = append(batches, row[0]) // Assuming each row is represented as a single comma-separated string
// 		}
// 	}

// 	return batches, nil
// }

func rowToString(row []string) string {
	return strings.Join(row, ",")
}
