package config

import (
	"context"

	"github.com/jackc/pgx/v4"
)

func ConnectDB() (*pgx.Conn, error) {
	conn, err := pgx.Connect(context.Background(), "postgresql://postgres:hadimas@localhost:5432/product")
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func SaveToDatabase(conn *pgx.Conn, data []string) error {
	// Assuming data contains fields that match the columns in your product table
	_, err := conn.Exec(context.Background(), "INSERT INTO product (product, qty, price) VALUES ($1, $2, $3)", data[0], data[1], data[2])
	return err
}
