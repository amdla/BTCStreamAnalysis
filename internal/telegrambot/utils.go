package telegrambot

import (
	"app/internal/models"
	"fmt"
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

func ValidateTrade(trade models.BinanceTradeData) bool {
	return trade.Price*trade.Quantity > 1_500_000
}
