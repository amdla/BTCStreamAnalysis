package minio

import (
	"context"
	"io"
	"log/slog"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Client struct {
	Config      *Config
	Logger      *slog.Logger
	MinioClient *minio.Client
}

func NewMinioClient() *Client {
	config := InitializeMinioConfig()

	logger := InitializeMinioLogger(config.IsDebugMode)

	minioClient, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: false,
	})
	if err != nil {
		logger.Error("Failed to create Minio client", slog.Any("error", err))
		minioClient = nil
	}

	return &Client{
		Config:      config,
		Logger:      logger,
		MinioClient: minioClient,
	}
}

func InitializeMinioLogger(debug bool) *slog.Logger {
	var level slog.Level
	if debug {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	return logger
}

func (c *Client) Insert(bucketName, objectName string, reader io.Reader, objectSize int64) (minio.UploadInfo, error) {
	return c.MinioClient.PutObject(context.Background(), bucketName, objectName, reader, objectSize, minio.PutObjectOptions{})
}

func (c *Client) MakeBucketOptions() minio.MakeBucketOptions {
	return minio.MakeBucketOptions{}
}
