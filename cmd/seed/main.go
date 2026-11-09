package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

type entity struct {
	address, name, category string
}

// ENTITIES_FILE is a CSV with address,entity_name,category. Keep the source
// file under operational configuration so OFAC updates do not require a build.
func readEntities(path string) ([]entity, error) {
	if path == "" {
		return nil, fmt.Errorf("ENTITIES_FILE is required (CSV: address,entity_name,category)")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open entities file: %w", err)
	}
	defer file.Close()
	var result []entity
	scanner := bufio.NewScanner(file)
	for line := 1; scanner.Scan(); line++ {
		fields := strings.Split(scanner.Text(), ",")
		if len(fields) != 3 || !strings.HasPrefix(fields[0], "0x") || len(fields[0]) != 42 {
			return nil, fmt.Errorf("invalid entity row %d", line)
		}
		result = append(result, entity{strings.ToLower(fields[0]), fields[1], fields[2]})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read entities file: %w", err)
	}
	return result, nil
}

func main() {
	ctx := context.Background()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	entities, err := readEntities(os.Getenv("ENTITIES_FILE"))
	if err != nil {
		log.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatal(err)
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()
	pipe := rdb.Pipeline()
	for _, item := range entities {
		if _, err := conn.Exec(ctx, `INSERT INTO entities(address, entity_name, category, is_high_degree)
			VALUES ($1, $2, $3, TRUE)
			ON CONFLICT (address) DO UPDATE SET entity_name = EXCLUDED.entity_name,
			category = EXCLUDED.category, is_high_degree = EXCLUDED.is_high_degree`,
			item.address, item.name, item.category); err != nil {
			log.Fatal(err)
		}
		if item.category == "SANCTIONED_OFAC" || item.category == "MIXER" {
			pipe.SAdd(ctx, "sanctions:sdn", item.address)
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("seeded %d entities\n", len(entities))
}
