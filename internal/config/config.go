package config

type Config struct {
	Env string `env:"APP_ENV" envDefault:"dev"`

	Postgres PostgresConfig
	Server   ServerConfig
	Logging  LoggingConfig
	Otel     OtelConfig
	Relay    RelayConfig
	NATS     NATSConfig
	Consumer ConsumerConfig
	Rule     RuleConfig
}

type PostgresConfig struct {
	Host               string `env:"DB_HOST,required"`
	Port               int    `env:"DB_PORT" envDefault:"5432"`
	User               string `env:"DB_USER,required"`
	Password           string `env:"DB_PASSWORD,required"`
	DBName             string `env:"DB_NAME,required"`
	MaxConns           int    `env:"DB_MAX_CONNECTIONS" envDefault:"1"`
	MinConns           int    `env:"DB_MIN_CONNECTIONS" envDefault:"1"`
	MaxConnLifetimeSec int    `env:"DB_MAX_CONN_LIFETIME_SEC" envDefault:"300"`
	MaxConnIdleTimeSec int    `env:"DB_MAX_CONN_IDLE_TIME_SEC" envDefault:"300"`
	HealthTimeoutSec   int    `env:"DB_HEALTH_TIMEOUT_SEC" envDefault:"5"`
}

type ServerConfig struct {
	Host           string `env:"SERVER_HOST" envDefault:"0.0.0.0"`
	Port           int    `env:"SERVER_PORT" envDefault:"5972"`
	MaxConcurrency int64  `env:"SERVER_MAX_CONCURRENCY" envDefault:"1000"`
	ReadTimeout    int    `env:"SERVER_READ_TIMEOUT_SEC" envDefault:"5"`
	WriteTimeout   int    `env:"SERVER_WRITE_TIMEOUT_SEC" envDefault:"10"`
	IdleTimeout    int    `env:"SERVER_IDLE_TIMEOUT_SEC" envDefault:"120"`
}

type LoggingConfig struct {
	Level string `env:"LOG_LEVEL" envDefault:"info"`
}

type OtelConfig struct {
	ExporterEndpoint string `env:"OTEL_EXPORTER_ENDPOINT" envDefault:"http://localhost:4317"`
	ServiceName      string `env:"OTEL_SERVICE_NAME" envDefault:"relay"`
}

type RelayConfig struct {
	SlotName       string `env:"RELAY_SLOT_NAME" envDefault:"relay_slot"`
	StatusInterval int    `env:"RELAY_STATUS_INTERVAL_SEC" envDefault:"10"`
	OutboxTable    string `env:"RELAY_OUTBOX_TABLE" envDefault:"outbox_events"`
	MaxRetries     int    `env:"RELAY_MAX_RETRIES" envDefault:"3"`
	Workers        int    `env:"RELAY_WORKERS" envDefault:"3"`
}

type ConsumerConfig struct {
	Workers         int `env:"CONSUMER_WORKERS" envDefault:"4"`
	BatchSize       int `env:"CONSUMER_BATCH_SIZE" envDefault:"10"`
	FetchTimeoutSec int `env:"CONSUMER_FETCH_TIMEOUT_SEC" envDefault:"5"`
}

type RuleConfig struct {
	RefreshIntervalSec int `env:"RULE_REFRESH_INTERVAL_SEC" envDefault:"30"`
}

type NATSConfig struct {
	URL               string `env:"NATS_URL,required"`
	StreamName        string `env:"NATS_STREAM_NAME" envDefault:"RELAY"`
	Subject           string `env:"NATS_SUBJECT" envDefault:"relay.events"`
	ConnectTimeoutSec int    `env:"NATS_CONNECT_TIMEOUT_SEC" envDefault:"5"`
	PublishTimeoutSec int    `env:"NATS_PUBLISH_TIMEOUT_SEC" envDefault:"5"`
	MaxReconnects     int    `env:"NATS_MAX_RECONNECTS" envDefault:"-1"`
	ReconnectWaitMs   int    `env:"NATS_RECONNECT_WAIT_MS" envDefault:"2000"`
	DrainTimeoutSec   int    `env:"NATS_DRAIN_TIMEOUT_SEC" envDefault:"10"`
	ConsumerGroup     string `env:"NATS_CONSUMER_GROUP" envDefault:"relay-consumer"`
}
