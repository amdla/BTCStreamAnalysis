package analytics

import (
	"app/internal/models"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

const defaultRowGroupSize = 4 * 1024 * 1024

func writeMinuteParquet(rows []*models.MinuteAnalyticsRow) ([]byte, error) {
	buf := buffer.NewBufferFile()

	pw, err := writer.NewParquetWriter(buf, new(models.MinuteAnalyticsRow), 1)
	if err != nil {
		return nil, err
	}

	pw.RowGroupSize = defaultRowGroupSize

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

func writeHourlyParquet(rows []*models.HourlyAnalyticsRow) ([]byte, error) {
	buf := buffer.NewBufferFile()

	pw, err := writer.NewParquetWriter(buf, new(models.HourlyAnalyticsRow), 1)
	if err != nil {
		return nil, err
	}

	pw.RowGroupSize = defaultRowGroupSize

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

func writeDailyParquet(rows []*models.DailyAnalyticsRow) ([]byte, error) {
	buf := buffer.NewBufferFile()

	pw, err := writer.NewParquetWriter(buf, new(models.DailyAnalyticsRow), 1)
	if err != nil {
		return nil, err
	}

	pw.RowGroupSize = defaultRowGroupSize

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
