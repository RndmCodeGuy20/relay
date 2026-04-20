package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"rndmcodeguy.in/relay/internal/config"
	"rndmcodeguy.in/relay/internal/logger"
	"rndmcodeguy.in/relay/internal/postgres"
	"rndmcodeguy.in/relay/internal/postgres/migrations"
)

func main() {
	log := logger.New(logger.Config{
		Level:       zapcore.DebugLevel,
		ServiceName: "migrate",
	})
	if len(os.Args) < 2 {
		panic("usage: migrate [up|down]")
	}

	command := os.Args[1]

	ctx := context.Background()

	log.Info("starting migration command", zap.String("command", command))
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	dsn := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.DBName,
	)

	// --- DB CONFIG ---
	db, err := postgres.New(ctx,
		postgres.Config{
			DSN:             dsn,
			MaxConns:        int32(cfg.Postgres.MaxConns),
			MinConns:        int32(cfg.Postgres.MinConns),
			MaxConnLifetime: time.Duration(cfg.Postgres.MaxConnLifetimeSec) * time.Second,
			MaxConnIdleTime: time.Duration(cfg.Postgres.MaxConnIdleTimeSec) * time.Second,
			HealthTimeout:   time.Duration(cfg.Postgres.HealthTimeoutSec) * time.Second,
		})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// --- LOAD MIGRATIONS ---
	migs, err := migrations.LoadFromDir("./migrations")
	if err != nil {
		panic(err)
	}

	runner := migrations.NewRunner(db.Pool(), nil)

	switch command {

	case "up":
		if err := runner.Up(ctx, migs); err != nil {
			panic(err)
		}
		fmt.Println("migrations applied")

	case "down":
		steps := 1
		if len(os.Args) > 2 {
			s, err := strconv.Atoi(os.Args[2])
			if err != nil {
				panic("invalid steps")
			}
			steps = s
		}

		if err := runner.Down(ctx, migs, steps); err != nil {
			panic(err)
		}
		fmt.Println("rolled back", steps, "migration(s)")

	default:
		panic("unknown command")
	}
}
