package config

import (
	"os"
	"strconv"
	"time"

	"go.yaml.in/yaml/v2"
)

type Config struct {
	HTTPAddr              string
	DatabaseURL           string
	AppEnv                string
	TelegramBotToken      string
	TelegramWebhookSecret string
	PaymentWebhookSecret  string
	MetricsAddr           string
	RedisAddr             string
	RedisPassword         string
	RedisDB               int
	SchedulerInterval     time.Duration
	SchedulerBatchSize    int
	WorkerConcurrency     int
	LogLevel              string
	CronConfigPath        string
}

type CronConfig struct {
	Jobs []CronJobConfig `yaml:"cron_jobs" json:"cron_jobs"`
}

type CronJobConfig struct {
	Name        string                 `yaml:"name" json:"name"`
	Schedule    string                 `yaml:"schedule" json:"schedule"`
	Type        string                 `yaml:"type" json:"type"`
	Payload     map[string]interface{} `yaml:"payload" json:"payload"`
	MaxAttempts int                    `yaml:"max_attempts" json:"max_attempts"`
}

func Load() *Config {
	return &Config{
		HTTPAddr:              getEnv("HTTP_ADDR", ":8080"),
		DatabaseURL:           getEnv("DATABASE_URL", "postgres://scheduler:scheduler@localhost:5432/scheduler?sslmode=disable"),
		AppEnv:                getEnv("APP_ENV", "local"),
		TelegramBotToken:      getEnv("TELEGRAM_BOT_TOKEN", ""),
		TelegramWebhookSecret: getEnv("TELEGRAM_WEBHOOK_SECRET", ""),
		PaymentWebhookSecret:  getEnv("PAYMENT_WEBHOOK_SECRET", ""),
		MetricsAddr:           getEnv("METRICS_ADDR", ":9090"),
		RedisAddr:             getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword:         getEnv("REDIS_PASSWORD", ""),
		RedisDB:               getEnvInt("REDIS_DB", 0),
		SchedulerInterval:     getEnvDuration("SCHEDULER_INTERVAL", time.Second),
		SchedulerBatchSize:    getEnvInt("SCHEDULER_BATCH_SIZE", 100),
		WorkerConcurrency:     getEnvInt("WORKER_CONCURRENCY", 10),
		LogLevel:              getEnv("LOG_LEVEL", "info"),
		CronConfigPath:        getEnv("CRON_CONFIG_PATH", "./cron.yaml"),
	}
}

func LoadCronConfig(path string) (CronConfig, error) {
	var cfg CronConfig
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, err
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	intVal, err := strconv.Atoi(val)
	if err != nil {
		return fallback
	}
	return intVal
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	val := os.Getenv(key)
	if val == "" {
		return fallback
	}
	durationVal, err := time.ParseDuration(val)
	if err != nil {
		return fallback
	}
	return durationVal
}
