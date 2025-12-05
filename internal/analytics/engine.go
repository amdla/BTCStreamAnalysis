package analytics

import (
	"app/internal/models"
	"fmt"
	"time"
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

type Uploader interface {
	Upload(objectName string, data []byte) error
}

func NewEngine(cfg Config, uploader Uploader) *Engine {
	return &Engine{
		cfg:      cfg,
		uploader: uploader,
		runID:    fmt.Sprintf("run-%d", time.Now().UTC().UnixNano()),
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
	partitions := buildMinutePartitions(trades)
	for key, rows := range partitions {
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
	partitions := buildHourlyPartitions(trades)
	for key, rows := range partitions {
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
	partitions := buildDailyPartitions(trades)
	for key, rows := range partitions {
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
