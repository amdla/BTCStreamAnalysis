package analytics

import (
	"app/internal/models"
	"sort"
	"time"
)

type partitionKey struct {
	symbol string
	date   string
}

func buildMinutePartitions(trades []models.ProcessedTrade) map[partitionKey][]*models.MinuteAnalyticsRow {
	type minuteKey struct {
		symbol string
		minute time.Time
	}

	buckets := make(map[minuteKey]*minuteBucket)

	for _, tr := range trades {
		minuteStart := tr.TradeTime.UTC().Truncate(time.Minute)

		key := minuteKey{
			symbol: tr.Symbol,
			minute: minuteStart,
		}

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

	sortMinutePartitions(partitions)

	return partitions
}

func sortMinutePartitions(partitions map[partitionKey][]*models.MinuteAnalyticsRow) {
	for _, rows := range partitions {
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].MinuteStartMillis < rows[j].MinuteStartMillis
		})
	}
}

func buildHourlyPartitions(trades []models.ProcessedTrade) map[partitionKey][]*models.HourlyAnalyticsRow {
	type hourKey struct {
		symbol string
		hour   time.Time
	}

	buckets := make(map[hourKey]*hourlyBucket)

	for _, tr := range trades {
		hourStart := tr.TradeTime.UTC().Truncate(time.Hour)

		key := hourKey{
			symbol: tr.Symbol,
			hour:   hourStart,
		}

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

	sortHourlyPartitions(partitions)

	return partitions
}

func sortHourlyPartitions(partitions map[partitionKey][]*models.HourlyAnalyticsRow) {
	for _, rows := range partitions {
		sort.Slice(rows, func(i, j int) bool {
			return rows[i].HourStartMillis < rows[j].HourStartMillis
		})
	}
}

func buildDailyPartitions(trades []models.ProcessedTrade) map[partitionKey][]*models.DailyAnalyticsRow {
	buckets := make(map[partitionKey]*dailyBucket)

	for _, tr := range trades {
		dayStart := tr.TradeTime.UTC().Truncate(24 * time.Hour)

		key := partitionKey{
			symbol: tr.Symbol,
			date:   dayStart.Format(timeLayout),
		}

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
