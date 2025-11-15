package resolvers

import (
	"app/graph/model"
	"app/internal/repository"
)

func mapSortDir(dir *model.SortDirection) *repository.SortDirection {
	if dir == nil {
		return nil
	}
	var v repository.SortDirection
	switch *dir {
	case model.SortDirectionAsc:
		v = repository.SortDirection(1)
	case model.SortDirectionDesc:
		v = repository.SortDirection(-1)
	default:
		return nil
	}
	return &v
}

func mapBinanceFilter(f *model.BinanceTradeFilter) *repository.BinanceTradeFilter {
	if f == nil {
		return &repository.BinanceTradeFilter{}
	}

	r := &repository.BinanceTradeFilter{
		ID:           f.ID,
		Stream:       f.Stream,
		EventType:    f.EventType,
		AggregateID:  f.AggregateID,
		Symbol:       f.Symbol,
		FirstTradeID: f.FirstTradeID,
		LastTradeID:  f.LastTradeID,
		IsBuyerMaker: f.IsBuyerMaker,
		Regex:        f.Regex,
		Limit:        f.Limit,
		SortField1:   f.SortField1,
		SortDir1:     mapSortDir(f.SortDir1),
		SortField2:   f.SortField2,
		SortDir2:     mapSortDir(f.SortDir2),
	}

	if f.Price != nil {
		r.Price = f.Price.Eq
		r.PriceGT = f.Price.Gt
		r.PriceGTE = f.Price.Gte
		r.PriceLT = f.Price.Lt
		r.PriceLTE = f.Price.Lte
		r.PriceNE = f.Price.Ne
	}

	if f.Quantity != nil {
		r.Quantity = f.Quantity.Eq
		r.QuantityGT = f.Quantity.Gt
		r.QuantityGTE = f.Quantity.Gte
		r.QuantityLT = f.Quantity.Lt
		r.QuantityLTE = f.Quantity.Lte
		r.QuantityNE = f.Quantity.Ne
	}

	if f.EventTime != nil {
		r.EventTimeAfter = f.EventTime.After
		r.EventTimeBefore = f.EventTime.Before
	}

	if f.TradeTime != nil {
		r.TradeTimeAfter = f.TradeTime.After
		r.TradeTimeBefore = f.TradeTime.Before
	}

	return r
}

func mapNotificationFilter(f *model.NotificationFilter) *repository.NotificationFilter {
	if f == nil {
		return &repository.NotificationFilter{}
	}

	r := &repository.NotificationFilter{
		EventTime:    f.EventTime,
		Symbol:       f.Symbol,
		IsBuyerMaker: f.IsBuyerMaker,
		Regex:        f.Regex,
		Limit:        f.Limit,
		SortField1:   f.SortField1,
		SortDir1:     mapSortDir(f.SortDir1),
		SortField2:   f.SortField2,
		SortDir2:     mapSortDir(f.SortDir2),
	}

	if f.Price != nil {
		r.Price = f.Price.Eq
		r.PriceGT = f.Price.Gt
		r.PriceGTE = f.Price.Gte
		r.PriceLT = f.Price.Lt
		r.PriceLTE = f.Price.Lte
		r.PriceNE = f.Price.Ne
	}

	if f.Quantity != nil {
		r.Quantity = f.Quantity.Eq
		r.QuantityGT = f.Quantity.Gt
		r.QuantityGTE = f.Quantity.Gte
		r.QuantityLT = f.Quantity.Lt
		r.QuantityLTE = f.Quantity.Lte
		r.QuantityNE = f.Quantity.Ne
	}

	if f.TotalPrice != nil {
		r.TotalPrice = f.TotalPrice.Eq
		r.TotalPriceGT = f.TotalPrice.Gt
		r.TotalPriceGTE = f.TotalPrice.Gte
		r.TotalPriceLT = f.TotalPrice.Lt
		r.TotalPriceLTE = f.TotalPrice.Lte
		r.TotalPriceNE = f.TotalPrice.Ne
	}

	return r
}
