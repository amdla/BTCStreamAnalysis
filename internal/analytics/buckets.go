package analytics

import (
	"app/internal/models"
	"time"
)

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
