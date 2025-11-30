package analytics

import (
	"app/internal/models"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

const (
	modeMinute = "minute"
	modeHourly = "hourly"
	modeDaily  = "daily"
	timeLayout = "2006-01-02"
)

type Config struct {
	AnalyticsPath string
}

type Engine struct {
	cfg      Config
	uploader Uploader
	runID    string
}

func NewEngine(cfg Config, uploader Uploader) *Engine {
	return &Engine{cfg: cfg, uploader: uploader, runID: fmt.Sprintf("run-%d", time.Now().UTC().UnixNano())}
}

type Uploader interface {
	Upload(objectName string, data []byte) error
}

type partitionKey struct {
	symbol string
	date   string
}

type aggStats struct {
	minPrice   float64
	maxPrice   float64
	volume     float64
	tradeCnt   int64
	notifCnt   int64
	priceSum   float64
	priceCount int64
	firstTs    time.Time
	lastTs     time.Time
	firstPrice float64
	lastPrice  float64
	seen       bool
}

func newAggStats() aggStats {
	return aggStats{
		minPrice: math.MaxFloat64,
		maxPrice: -math.MaxFloat64,
	}
}

func (s *aggStats) add(tr models.ProcessedTrade) {
	if tr.Price < s.minPrice {
		s.minPrice = tr.Price
	}
	if tr.Price > s.maxPrice {
		s.maxPrice = tr.Price
	}
	if !s.seen || tr.TradeTime.Before(s.firstTs) {
		s.firstTs = tr.TradeTime
		s.firstPrice = tr.Price
	}
	if !s.seen || tr.TradeTime.After(s.lastTs) {
		s.lastTs = tr.TradeTime
		s.lastPrice = tr.Price
	}

	s.volume += tr.Quantity
	s.tradeCnt++
	if tr.TriggeredNotification {
		s.notifCnt++
	}
	s.priceSum += tr.Price
	s.priceCount++
	s.seen = true
}

func (s aggStats) avgPrice() float64 {
	if s.priceCount == 0 {
		return 0
	}
	return s.priceSum / float64(s.priceCount)
}

type hourlyBucket struct {
	symbol string
	hour   time.Time
	stats  aggStats
}

func newHourlyBucket(symbol string, hour time.Time) *hourlyBucket {
	return &hourlyBucket{symbol: symbol, hour: hour.UTC(), stats: newAggStats()}
}

func (b *hourlyBucket) add(tr models.ProcessedTrade) {
	b.stats.add(tr)
}

func (b *hourlyBucket) row() *models.HourlyAnalyticsRow {
	if !b.stats.seen {
		return nil
	}
	date := b.hour.Format(timeLayout)
	return &models.HourlyAnalyticsRow{
		Symbol:                 b.symbol,
		Date:                   date,
		HourStartMillis:        b.hour.UnixMilli(),
		MinPrice:               b.stats.minPrice,
		MaxPrice:               b.stats.maxPrice,
		AvgPrice:               b.stats.avgPrice(),
		Volume:                 b.stats.volume,
		TradeCount:             b.stats.tradeCnt,
		NotificationsTriggered: b.stats.notifCnt,
		FirstPrice:             b.stats.firstPrice,
		LastPrice:              b.stats.lastPrice,
	}
}

type dailyBucket struct {
	symbol string
	day    time.Time
	stats  aggStats
}

func newDailyBucket(symbol string, day time.Time) *dailyBucket {
	return &dailyBucket{symbol: symbol, day: day.UTC(), stats: newAggStats()}
}

func (b *dailyBucket) add(tr models.ProcessedTrade) {
	b.stats.add(tr)
}

func (b *dailyBucket) row() *models.DailyAnalyticsRow {
	if !b.stats.seen {
		return nil
	}
	date := b.day.Format(timeLayout)
	return &models.DailyAnalyticsRow{
		Symbol:                 b.symbol,
		Date:                   date,
		DayStartMillis:         b.day.UnixMilli(),
		MinPrice:               b.stats.minPrice,
		MaxPrice:               b.stats.maxPrice,
		AvgPrice:               b.stats.avgPrice(),
		Volume:                 b.stats.volume,
		TradeCount:             b.stats.tradeCnt,
		NotificationsTriggered: b.stats.notifCnt,
		FirstPrice:             b.stats.firstPrice,
		LastPrice:              b.stats.lastPrice,
	}
}

type minuteBucket struct {
	symbol string
	minute time.Time
	stats  aggStats
}

func newMinuteBucket(symbol string, minute time.Time) *minuteBucket {
	return &minuteBucket{symbol: symbol, minute: minute.UTC(), stats: newAggStats()}
}

func (b *minuteBucket) add(tr models.ProcessedTrade) {
	b.stats.add(tr)
}

func (b *minuteBucket) row() *models.MinuteAnalyticsRow {
	if !b.stats.seen {
		return nil
	}
	date := b.minute.Format(timeLayout)
	return &models.MinuteAnalyticsRow{
		Symbol:                 b.symbol,
		Date:                   date,
		MinuteStartMillis:      b.minute.UnixMilli(),
		MinPrice:               b.stats.minPrice,
		MaxPrice:               b.stats.maxPrice,
		AvgPrice:               b.stats.avgPrice(),
		Volume:                 b.stats.volume,
		TradeCount:             b.stats.tradeCnt,
		NotificationsTriggered: b.stats.notifCnt,
		FirstPrice:             b.stats.firstPrice,
		LastPrice:              b.stats.lastPrice,
	}
}

func (e *Engine) Process(trades []models.ProcessedTrade) error {
	if len(trades) == 0 {
		return nil
	}

	if err := e.buildAndUploadMinute(trades); err != nil {
		return err
	}
	if err := e.buildAndUploadHourly(trades); err != nil {
		return err
	}
	return e.buildAndUploadDaily(trades)
}

func (e *Engine) buildAndUploadMinute(trades []models.ProcessedTrade) error {
	minutePartitions := buildMinutePartitions(trades)
	for key, rows := range minutePartitions {
		payload, err := writeMinuteParquet(rows)
		if err != nil {
			return err
		}

		objectName := BuildAnalyticsObject(e.cfg.AnalyticsPath, modeMinute, key.symbol, key.date, e.runID)
		if err := e.uploader.Upload(objectName, payload); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) buildAndUploadHourly(trades []models.ProcessedTrade) error {
	hourlyPartitions := buildHourlyPartitions(trades)
	for key, rows := range hourlyPartitions {
		payload, err := writeHourlyParquet(rows)
		if err != nil {
			return err
		}

		objectName := BuildAnalyticsObject(e.cfg.AnalyticsPath, modeHourly, key.symbol, key.date, e.runID)
		if err := e.uploader.Upload(objectName, payload); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) buildAndUploadDaily(trades []models.ProcessedTrade) error {
	dailyPartitions := buildDailyPartitions(trades)
	for key, rows := range dailyPartitions {
		payload, err := writeDailyParquet(rows)
		if err != nil {
			return err
		}

		objectName := BuildAnalyticsObject(e.cfg.AnalyticsPath, modeDaily, key.symbol, key.date, e.runID)
		if err := e.uploader.Upload(objectName, payload); err != nil {
			return err
		}
	}
	return nil
}

//goland:noinspection DuplicatedCode,DuplicatedCode
func buildMinutePartitions(trades []models.ProcessedTrade) map[partitionKey][]*models.MinuteAnalyticsRow {
	type minuteKey struct {
		symbol string
		minute time.Time
	}

	buckets := make(map[minuteKey]*minuteBucket)
	for _, tr := range trades {
		minuteStart := tr.TradeTime.UTC().Truncate(time.Minute)
		key := minuteKey{symbol: tr.Symbol, minute: minuteStart}
		bucket, ok := buckets[key]
		if !ok {
			bucket = newMinuteBucket(tr.Symbol, minuteStart)
			buckets[key] = bucket
		}
		bucket.add(tr)
	}

	partitions := make(map[partitionKey][]*models.MinuteAnalyticsRow)
	for _, bucket := range buckets {
		row := bucket.row()
		if row == nil {
			continue
		}
		pk := partitionKey{symbol: row.Symbol, date: row.Date}
		partitions[pk] = append(partitions[pk], row)
	}

	for _, rows := range partitions {
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].MinuteStartMillis < rows[j].MinuteStartMillis
		})
	}

	return partitions
}

func buildHourlyPartitions(trades []models.ProcessedTrade) map[partitionKey][]*models.HourlyAnalyticsRow {
	type hourKey struct {
		symbol string
		hour   time.Time
	}

	buckets := make(map[hourKey]*hourlyBucket)
	for _, tr := range trades {
		hourStart := tr.TradeTime.UTC().Truncate(time.Hour)
		key := hourKey{symbol: tr.Symbol, hour: hourStart}
		bucket, ok := buckets[key]
		if !ok {
			bucket = newHourlyBucket(tr.Symbol, hourStart)
			buckets[key] = bucket
		}
		bucket.add(tr)
	}

	partitions := make(map[partitionKey][]*models.HourlyAnalyticsRow)
	for _, bucket := range buckets {
		row := bucket.row()
		if row == nil {
			continue
		}
		pk := partitionKey{symbol: row.Symbol, date: row.Date}
		partitions[pk] = append(partitions[pk], row)
	}

	for _, rows := range partitions {
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].HourStartMillis < rows[j].HourStartMillis
		})
	}

	return partitions
}

func buildDailyPartitions(trades []models.ProcessedTrade) map[partitionKey][]*models.DailyAnalyticsRow {
	buckets := make(map[partitionKey]*dailyBucket)
	for _, tr := range trades {
		dayStart := tr.TradeTime.UTC().Truncate(24 * time.Hour)
		key := partitionKey{symbol: tr.Symbol, date: dayStart.Format(timeLayout)}
		bucket, ok := buckets[key]
		if !ok {
			bucket = newDailyBucket(tr.Symbol, dayStart)
			buckets[key] = bucket
		}
		bucket.add(tr)
	}

	partitions := make(map[partitionKey][]*models.DailyAnalyticsRow)
	for key, bucket := range buckets {
		row := bucket.row()
		if row == nil {
			continue
		}
		partitions[key] = []*models.DailyAnalyticsRow{row}
	}

	return partitions
}

func writeMinuteParquet(rows []*models.MinuteAnalyticsRow) ([]byte, error) {
	buf := buffer.NewBufferFile()
	pw, err := writer.NewParquetWriter(buf, new(models.MinuteAnalyticsRow), 1)
	if err != nil {
		return nil, err
	}
	pw.RowGroupSize = 4 * 1024 * 1024
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
	pw.RowGroupSize = 4 * 1024 * 1024
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
	pw.RowGroupSize = 4 * 1024 * 1024
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
