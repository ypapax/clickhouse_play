package main

import (
	"context"
	"github.com/ClickHouse/clickhouse-go/v2"
	"log"
	"os"
	"time"
)

func main() {
	opts := clickhouse.Options{
		Addr: []string{os.Getenv("CLICKHOUSE_HOST") + ":" + os.Getenv("CLICKHOUSE_PORT")},
		Auth: clickhouse.Auth{
			Database: os.Getenv("CLICKHOUSE_DATABASE"),
			Username: os.Getenv("CLICKHOUSE_USER"),
			Password: os.Getenv("CLICKHOUSE_PASSWORD"),
		},
	}
	log.Println("Connecting to ClickHouse", opts)
	conn, err := clickhouse.Open(&opts)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	// Создаем таблицу если её нет
	err = conn.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS events (
            timestamp DateTime,
            message String
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    `)
	if err != nil {
		log.Fatal(err)
	}

	const batchSize = 10000 // Размер пакета
	batch := make([]struct {
		Timestamp time.Time
		Message   string
	}, 0, batchSize)

	for {
		// Накапливаем данные для пакета
		batch = append(batch, struct {
			Timestamp time.Time
			Message   string
		}{
			Timestamp: time.Now(),
			Message:   "Hello from Go!",
		})

		// Когда накопили нужное количество - вставляем
		if len(batch) >= batchSize {
			// Подготавливаем batch запрос
			batch_query, err := conn.PrepareBatch(ctx, "INSERT INTO events")
			if err != nil {
				log.Printf("Error PrepareBatch: %v", err)
				continue
			}

			for _, record := range batch {
				err := batch_query.Append(record.Timestamp, record.Message)
				if err != nil {
					log.Printf("Error appending to batch: %v", err)
					continue
				}
			}

			// Выполняем batch запрос
			if err := batch_query.Send(); err != nil {
				log.Printf("Error sending batch: %v", err)
			} else {
				log.Printf("Successfully inserted batch of %d records", len(batch))
			}

			// Очищаем batch для следующей порции
			batch = batch[:0]
		}

		time.Sleep(time.Millisecond) // Небольшая задержка
	}
}
