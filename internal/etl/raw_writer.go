package etl

import (
	"app/internal/models"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"time"

	minioSDK "github.com/minio/minio-go/v7"
)

// create a partition path for raw trades (hourly partitioning).
func buildRawTradePartitionKey(prefix string, tradeTime time.Time) string {
	utc := tradeTime.UTC()

	return fmt.Sprintf("%s/%04d/%02d/%02d/%02d", prefix, utc.Year(), utc.Month(), utc.Day(), utc.Hour())
}

// create a partition path for raw notifications (daily partitioning).
func buildRawNotificationPartitionKey(prefix string, eventTime time.Time) string {
	utc := eventTime.UTC()

	return fmt.Sprintf("%s/%04d/%02d/%02d", prefix, utc.Year(), utc.Month(), utc.Day())
}

// create a unique object path with timestamp to avoid collisions.
func buildRawObjectPath(partitionKey string) string {
	return fmt.Sprintf("%s/%d.jsonl.gz", partitionKey, time.Now().UTC().UnixNano())
}

func buildJSONLPayloadFromTrades(trades []models.BinanceTradeData) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	for i, trade := range trades {
		if err := encoder.Encode(trade); err != nil {
			return nil, fmt.Errorf("encode trade jsonl: %w", err)
		}

		if i < len(trades)-1 {
			buf.Truncate(buf.Len() - 1)
		}
	}

	return buf.Bytes(), nil
}

func buildJSONLPayloadFromNotifications(notifs []models.NotificationData) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	for i, notif := range notifs {
		if err := encoder.Encode(notif); err != nil {
			return nil, fmt.Errorf("encode notification jsonl: %w", err)
		}

		if i < len(notifs)-1 {
			buf.Truncate(buf.Len() - 1)
		}
	}

	return buf.Bytes(), nil
}

func compressPayloadToGzip(payload []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(payload); err != nil {
		return nil, err
	}

	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func newJSONLGzipOptions() minioSDK.PutObjectOptions {
	return minioSDK.PutObjectOptions{
		ContentType:     "application/jsonl",
		ContentEncoding: "gzip",
	}
}
