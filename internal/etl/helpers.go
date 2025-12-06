package etl

import (
	"app/internal/models"
	"app/internal/repository"
	"time"
)

type notificationMatch struct {
	data     models.NotificationData
	consumed bool
}

func notificationKeysForTrade(trade models.BinanceTradeData) []repository.NotificationKey {
	keys := []repository.NotificationKey{
		{Symbol: trade.Symbol, EventTime: trade.EventTime.UTC().Format(time.RFC3339Nano)},
	}

	standard := repository.NotificationKey{Symbol: trade.Symbol, EventTime: trade.EventTime.UTC().Format(time.RFC3339)}
	if standard.EventTime != keys[0].EventTime {
		keys = append(keys, standard)
	}

	legacy := repository.NotificationKey{Symbol: trade.Symbol, EventTime: trade.EventTime.String()}
	if legacy.EventTime != keys[0].EventTime && legacy.EventTime != standard.EventTime {
		keys = append(keys, legacy)
	}

	return keys
}

func hasNotificationForTrade(idx map[repository.NotificationKey]*notificationMatch, trade models.BinanceTradeData) bool {
	for _, key := range notificationKeysForTrade(trade) {
		if match, ok := idx[key]; ok && !match.consumed {
			match.consumed = true

			return true
		}
	}

	return false
}

func extractTradeIDs(trades []models.BinanceTradeData) []string {
	ids := make([]string, len(trades))

	for i, t := range trades {
		ids[i] = t.ID
	}

	return ids
}

func extractNotificationKeys(idx map[repository.NotificationKey]*notificationMatch) []repository.NotificationKey {
	keys := make([]repository.NotificationKey, 0, len(idx))

	for key := range idx {
		keys = append(keys, key)
	}

	return keys
}

func uniqueNotificationKeys(trades []models.BinanceTradeData) []repository.NotificationKey {
	set := make(map[repository.NotificationKey]struct{}, len(trades))

	for _, trade := range trades {
		for _, key := range notificationKeysForTrade(trade) {
			set[key] = struct{}{}
		}
	}

	result := make([]repository.NotificationKey, 0, len(set))

	for key := range set {
		result = append(result, key)
	}

	return result
}
