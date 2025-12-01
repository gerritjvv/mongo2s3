package internal

import (
	"time"
)

type Config struct {
	OTEL_COLLECTOR_URL     string            `yaml:"otel_collector_url"`
	DB_URL                 string            `yaml:"db_url"`
	MONGO_URL              string            `yaml:"mongo_url"`
	LocalBaseDir           string            `yaml:"local_base_dir"`
	S3Bucket               string            `yaml:"s3_bucket"`
	S3Region               string            `yaml:"s3_region"`
	S3Prefix               string            `yaml:"s3_prefix"`
	FileWriteTimeoutSecond time.Duration     `yaml:"file_write_timeout_second"`
	FileWriteSizeBytes     int64             `yaml:"file_write_size_bytes"`
	Collections            []MongoCollection `yaml:"collections"`
}

type MongoCollection struct {
	DB   string `yaml:"db"`
	Name string `yaml:"name"`
}
