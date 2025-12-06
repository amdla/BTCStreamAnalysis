package analytics

import (
	"app/internal/models"
	"math"
	"time"
)

// aggStats holds aggregated statistics for a time bucket.
type aggStats struct {
	minPrice   float64
	maxPrice   float64
	volume     float64
	tradeCnt   int64
	notifCnt   int64
	priceSum   float64
	priceCount int64
	firstTS    time.Time
	lastTS     time.Time
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

	if !s.seen || tr.TradeTime.Before(s.firstTS) {
		s.firstTS = tr.TradeTime
		s.firstPrice = tr.Price
	}

	if !s.seen || tr.TradeTime.After(s.lastTS) {
		s.lastTS = tr.TradeTime
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
