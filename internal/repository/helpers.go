package repository

import (
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

type SortDirection int

func buildNumericRange(eq, gt, gte, lt, lte, ne *float64) bson.M {
	filter := bson.M{}

	if eq != nil {
		filter["$eq"] = *eq
	}

	if gt != nil {
		filter["$gt"] = *gt
	}

	if gte != nil {
		filter["$gte"] = *gte
	}

	if lt != nil {
		filter["$lt"] = *lt
	}

	if lte != nil {
		filter["$lte"] = *lte
	}

	if ne != nil {
		filter["$ne"] = *ne
	}

	return filter
}

func buildTimeRange(after, before *time.Time) bson.M {
	filter := bson.M{}

	if after != nil {
		filter["$gte"] = after.UTC().Format(time.RFC3339Nano)
	}

	if before != nil {
		filter["$lte"] = before.UTC().Format(time.RFC3339Nano)
	}

	return filter
}

func buildSortDocument(prefix string, field1, field2 *string, dir1, dir2 *SortDirection) bson.D {
	sortDoc := bson.D{}

	if field1 != nil && dir1 != nil {
		sortDoc = append(sortDoc, bson.E{Key: prefix + *field1, Value: *dir1})
	}

	if field2 != nil && dir2 != nil {
		sortDoc = append(sortDoc, bson.E{Key: prefix + *field2, Value: *dir2})
	}

	return sortDoc
}
