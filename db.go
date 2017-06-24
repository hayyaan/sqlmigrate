package db

import (
	"database/sql"

	_ "github.com/lib/pq"
)

// A DB is responsible for all interactions with postgres
type DB struct {
	sql *sql.DB
}

// NewDB returns a new database
func NewDB(dsn string) (*DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	err = runMigrations(db)
	if err != nil {
		return nil, err
	}

	return &DB{
		sql: db,
	}, nil
}
