package analytics

import (
	"bytes"
	"fmt"
	"io"
	"path"
	"strings"

	minioSDK "github.com/minio/minio-go/v7"
)

type MinioUploader struct {
	BucketName string
	Client     MinioClient
}

type MinioClient interface {
	Insert(bucketName, objectName string, reader io.Reader, objectSize int64, opts minioSDK.PutObjectOptions) (minioSDK.UploadInfo, error)
}

func (u *MinioUploader) Upload(objectName string, data []byte) error {
	reader := bytes.NewReader(data)
	_, err := u.Client.Insert(u.BucketName, objectName, reader, int64(len(data)), minioSDK.PutObjectOptions{ContentType: "application/octet-stream"})

	return err
}

func BuildAnalyticsObject(base, mode, symbol, date string, extras ...string) string {
	prefix := strings.TrimSuffix(base, "/")
	fileName := fmt.Sprintf("%s.parquet", mode)

	if len(extras) > 0 && extras[0] != "" {
		fileName = fmt.Sprintf("%s-%s", extras[0], fileName)
	}

	return path.Join(prefix, mode, fmt.Sprintf("symbol=%s", symbol), fmt.Sprintf("date=%s", date), fileName)
}
