package db

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

//go:generate go-bindata -pkg db -mode 0644 -modtime 499137600 -o db_migrations_generated.go ../schema/

func runMigrations(db *sql.DB) error {
	err := verifyMigrationsTable(db)
	if err != nil {
		return err
	}

	count, err := countMigrations(db)
	if err != nil {
		return err
	}

	fileNames := AssetNames()
	sort.Strings(fileNames)

	for i, file := range fileNames {
		// skip running ones we've clearly already ran
		if count > 0 {
			count--
			continue
		}

		migration := MustAsset(file)

		err := runMigration(i, migration, db)
		if err != nil {
			return err
		}

		cleanName := strings.TrimPrefix(file, "../schema/")
		err = recordMigration(cleanName, db)
		if err != nil {
			return err
		}
	}

	return nil
}

func verifyMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`CREATE EXTENSION IF NOT EXISTS pgcrypto;`)
	if err != nil {
		return err
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS migrations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    name TEXT NOT NULL UNIQUE CHECK (name <> '')
  );`)
	return err
}

func countMigrations(db *sql.DB) (int, error) {
	row := db.QueryRow(`SELECT count(*) FROM migrations;`)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func runMigration(num int, buf []byte, db *sql.DB) error {
	// queries := strings.Split(string(buf), ";")

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}

	// Running all queries in file at once
	// for i, q := range queries {
	_, err = tx.Exec(string(buf))
	if err != nil {
		return fmt.Errorf("migrator: migration %d failed: %s", num, err)
	}
	// }

	return tx.Commit()
}

func recordMigration(name string, db *sql.DB) error {
	_, err := db.Query("INSERT INTO migrations (name) VALUES ($1);", name)
	if err != nil {
		return err
	}

	return nil
}
