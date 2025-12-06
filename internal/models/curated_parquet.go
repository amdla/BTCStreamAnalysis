package models

type CuratedTradeParquet struct {
	Symbol                string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	TradeTimeMillis       int64   `parquet:"name=trade_time, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	Price                 float64 `parquet:"name=price, type=DOUBLE"`
	Quantity              float64 `parquet:"name=quantity, type=DOUBLE"`
	AmountOfTrades        int64   `parquet:"name=amount_of_trades, type=INT64"`
	IsBuyerMaker          bool    `parquet:"name=is_buyer_maker, type=BOOLEAN"`
	TriggeredNotification bool    `parquet:"name=triggered_notification, type=BOOLEAN"`
	Date                  string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
}

func NewCuratedTradeParquet(trade ProcessedTrade) *CuratedTradeParquet {
	utc := trade.TradeTime.UTC()

	return &CuratedTradeParquet{
		Symbol:                trade.Symbol,
		TradeTimeMillis:       utc.UnixMilli(),
		Price:                 trade.Price,
		Quantity:              trade.Quantity,
		AmountOfTrades:        trade.AmountOfTrades,
		IsBuyerMaker:          trade.IsBuyerMaker,
		TriggeredNotification: trade.TriggeredNotification,
		Date:                  utc.Format("2006-01-02"),
	}
}

type HourlyAnalyticsRow struct {
	Symbol                 string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Date                   string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
	HourStartMillis        int64   `parquet:"name=hour_start, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MinPrice               float64 `parquet:"name=min_price, type=DOUBLE"`
	MaxPrice               float64 `parquet:"name=max_price, type=DOUBLE"`
	AvgPrice               float64 `parquet:"name=avg_price, type=DOUBLE"`
	Volume                 float64 `parquet:"name=volume, type=DOUBLE"`
	TradeCount             int64   `parquet:"name=trade_count, type=INT64"`
	NotificationsTriggered int64   `parquet:"name=notifications_triggered, type=INT64"`
	FirstPrice             float64 `parquet:"name=first_price, type=DOUBLE"`
	LastPrice              float64 `parquet:"name=last_price, type=DOUBLE"`
}

type DailyAnalyticsRow struct {
	Symbol                 string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Date                   string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
	DayStartMillis         int64   `parquet:"name=day_start, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MinPrice               float64 `parquet:"name=min_price, type=DOUBLE"`
	MaxPrice               float64 `parquet:"name=max_price, type=DOUBLE"`
	AvgPrice               float64 `parquet:"name=avg_price, type=DOUBLE"`
	Volume                 float64 `parquet:"name=volume, type=DOUBLE"`
	TradeCount             int64   `parquet:"name=trade_count, type=INT64"`
	NotificationsTriggered int64   `parquet:"name=notifications_triggered, type=INT64"`
	FirstPrice             float64 `parquet:"name=first_price, type=DOUBLE"`
	LastPrice              float64 `parquet:"name=last_price, type=DOUBLE"`
}

type MinuteAnalyticsRow struct {
	Symbol                 string  `parquet:"name=symbol, type=BYTE_ARRAY, convertedtype=UTF8"`
	Date                   string  `parquet:"name=date, type=BYTE_ARRAY, convertedtype=UTF8"`
	MinuteStartMillis      int64   `parquet:"name=minute_start, type=INT64, convertedtype=TIMESTAMP_MILLIS"`
	MinPrice               float64 `parquet:"name=min_price, type=DOUBLE"`
	MaxPrice               float64 `parquet:"name=max_price, type=DOUBLE"`
	AvgPrice               float64 `parquet:"name=avg_price, type=DOUBLE"`
	Volume                 float64 `parquet:"name=volume, type=DOUBLE"`
	TradeCount             int64   `parquet:"name=trade_count, type=INT64"`
	NotificationsTriggered int64   `parquet:"name=notifications_triggered, type=INT64"`
	FirstPrice             float64 `parquet:"name=first_price, type=DOUBLE"`
	LastPrice              float64 `parquet:"name=last_price, type=DOUBLE"`
}
