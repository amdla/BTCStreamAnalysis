package telegrambot

import (
	"app/internal/streamserver"
	"fmt"
	"strconv"
	"strings"
)

func formatTotalPrice(price float64) string {
	s := fmt.Sprintf("%.2f", price)

	parts := strings.Split(s, ".")
	intPart := parts[0]
	fracPart := parts[1]

	isNegative := false
	if intPart[0] == '-' {
		isNegative = true
		intPart = intPart[1:]
	}

	var b strings.Builder
	l := len(intPart)
	for i, r := range intPart {
		b.WriteRune(r)
		digitsToTheRight := l - 1 - i
		if digitsToTheRight > 0 && digitsToTheRight%3 == 0 {
			b.WriteByte(' ')
		}
	}

	if isNegative {
		return "-" + b.String() + "." + fracPart
	}
	return b.String() + "." + fracPart
}

func ValidateTrade(trade streamserver.BinanceTradeData) bool {
	price, err1 := strconv.ParseFloat(trade.Price, 64)
	qty, err2 := strconv.ParseFloat(trade.Quantity, 64)

	if err1 != nil || err2 != nil {
		return false
	}

	if price*qty > 500_000 {
		return true
	}

	return false
}
