package etl

import (
	"app/internal/models"
	"app/internal/repository"
	"fmt"
	"strings"
	"time"

	minioSDK "github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

const curatedRowGroupSize = 128 * 1024 * 1024

func buildCuratedObjectPrefix(base, symbol, date string) string {
	return fmt.Sprintf("%s/symbol=%s/date=%s", strings.TrimSuffix(base, "/"), symbol, date)
}

func curateTrades(trades []models.BinanceTradeData, notifIdx map[repository.NotificationKey]*notificationMatch) []models.ProcessedTrade {
	processed := make([]models.ProcessedTrade, len(trades))

	for i, trade := range trades {
		triggered := hasNotificationForTrade(notifIdx, trade)
		processed[i] = (models.EnrichedTrade{BinanceTradeData: trade, TriggeredNotification: triggered}).ToProcessedTrade()
	}

	return processed
}

func writeCuratedParquet(rows []*models.CuratedTradeParquet) ([]byte, error) {
	buf := buffer.NewBufferFile()

	pw, err := writer.NewParquetWriter(buf, new(models.CuratedTradeParquet), 1)
	if err != nil {
		return nil, err
	}

	pw.RowGroupSize = curatedRowGroupSize
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, row := range rows {
		if err := pw.Write(row); err != nil {
			return nil, err
		}
	}

	if err := pw.WriteStop(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func newParquetPutOptions() minioSDK.PutObjectOptions {
	return minioSDK.PutObjectOptions{ContentType: "application/octet-stream"}
}

func partitionCuratedTrades(processed []models.ProcessedTrade, curatedPath string) map[string][]*models.CuratedTradeParquet {
	partitions := make(map[string][]*models.CuratedTradeParquet)

	for _, trade := range processed {
		parquetRow := models.NewCuratedTradeParquet(trade)
		objectPrefix := buildCuratedObjectPrefix(curatedPath, parquetRow.Symbol, parquetRow.Date)
		partitions[objectPrefix] = append(partitions[objectPrefix], parquetRow)
	}

	return partitions
}

func generateCuratedObjectName(prefix string) string {
	return fmt.Sprintf("%s/%d.parquet", prefix, time.Now().UTC().UnixNano())
}
