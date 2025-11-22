package main

import (
	"app/internal/minio"
	"context"
	"log/slog"
	"strings"
)

func main() {
	client := minio.NewMinioClient()
	minioClient := client.MinioClient
	logger := client.Logger

	if minioClient == nil {
		logger.Error("MinioClient is nil")
		return
	}

	logger.Info("Minio client created successfully")

	logger.Info("================= TEST =================")
	logger.Info("================= TEST =================")
	logger.Info("================= TEST =================")

	bucketName := "test-bucket"
	objectName := "test-object.txt"
	content := "Hello, MinIO!"

	exists, err := minioClient.BucketExists(context.Background(), bucketName)
	if err != nil {
		logger.Error("Failed to check if bucket exists", slog.Any("error", err))
		return
	}
	if !exists {
		err = minioClient.MakeBucket(context.Background(), bucketName, client.MakeBucketOptions())
		if err != nil {
			logger.Error("Failed to create bucket", slog.Any("error", err))
			return
		}
		logger.Info("Bucket created successfully", slog.String("bucketName", bucketName))
	}

	reader := strings.NewReader(content)
	_, err = client.Insert(bucketName, objectName, reader, int64(len(content)))
	if err != nil {
		logger.Error("Failed to insert object", slog.Any("error", err))
		return
	}

	logger.Info("Object inserted successfully", slog.String("bucketName", bucketName), slog.String("objectName", objectName))
}
