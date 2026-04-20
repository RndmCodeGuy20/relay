package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/caarlos0/env/v11"
	"github.com/joho/godotenv"
)

func Load() (*Config, error) {
	loadDotEnv()

	cfg := &Config{}
	if err := env.Parse(cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	return cfg, nil
}

func loadDotEnv() {
	appEnv := strings.ToLower(getEnv("APP_ENV", "dev"))

	files := []string{".env"}

	if appEnv == "test" {
		files = []string{".env.test", ".env"}
	}

	if appEnv == "dev" || appEnv == "local" {
		files = []string{".env.local", ".env"}
	}

	if err := godotenv.Load(files...); err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "warning: failed to load env files %v: %v\n", files, err)
	}
}

func getEnv(key, fallback string) string {
	if v := strings.TrimSpace(strings.ToLower(os.Getenv(key))); v != "" {
		return v
	}
	return fallback
}
