package etl

import (
	"app/internal/analytics"
	"app/internal/minio"
	"app/internal/models"
	"app/internal/mongo"
	"app/internal/repository"
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"
)

type Service struct {
	cfg         *Config
	logger      *slog.Logger
	mongoClient *mongo.Client
	minioClient *minio.Client
	tradeRepo   repository.BinanceTradeRepository
	notifRepo   repository.NotificationRepository
}

func NewService(cfg *Config, logger *slog.Logger, mongoClient *mongo.Client, minioClient *minio.Client) *Service {
	return &Service{
		cfg:         cfg,
		logger:      logger,
		mongoClient: mongoClient,
		minioClient: minioClient,
		tradeRepo:   mongoClient.BinanceTradeRepo,
		notifRepo:   mongoClient.NotificationRepo,
	}
}

func (s *Service) Run(ctx context.Context) error {
	if s.tradeRepo == nil || s.notifRepo == nil {
		return fmt.Errorf("mongo repositories not initialized")
	}

	ticker := time.NewTicker(s.cfg.RunInterval)
	defer ticker.Stop()

	s.logger.Info("ETL service started", slog.Duration("interval", s.cfg.RunInterval))

	if err := s.ensureBucket(ctx); err != nil {
		return err
	}

	s.runOnce(ctx)

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("ETL service stopping")

			return nil
		case <-ticker.C:
			s.runOnce(ctx)
		}
	}
}

func (s *Service) runOnce(ctx context.Context) {
	var aggregated []models.ProcessedTrade

	for {
		batch, err := s.processBatch(ctx)
		if err != nil {
			s.logger.Error("Batch processing failed", slog.Any("error", err))

			break
		}

		if len(batch) > 0 {
			aggregated = append(aggregated, batch...)
		}

		hasMore, err := s.hasMoreTrades(ctx)
		if err != nil {
			s.logger.Error("Failed to check remaining trades", slog.Any("error", err))

			break
		}

		if !hasMore {
			break
		}
	}

	if len(aggregated) == 0 {
		return
	}

	if err := s.runAnalytics(aggregated); err != nil {
		s.logger.Error("Analytics processing failed", slog.Any("error", err))
	}
}

func (s *Service) ensureBucket(ctx context.Context) error {
	minioClient := s.minioClient.MinioClient
	if minioClient == nil {
		return fmt.Errorf("minio client not initialized")
	}

	exists, err := minioClient.BucketExists(ctx, s.cfg.BucketName)
	if err != nil {
		return err
	}

	if !exists {
		if err := minioClient.MakeBucket(ctx, s.cfg.BucketName, s.minioClient.MakeBucketOptions()); err != nil {
			return err
		}

		s.logger.Info("Created MinIO bucket", slog.String("bucket", s.cfg.BucketName))
	}

	return nil
}

func (s *Service) processBatch(ctx context.Context) ([]models.ProcessedTrade, error) {
	trades, err := s.fetchTrades(ctx)
	if err != nil {
		return nil, err
	}

	if len(trades) == 0 {
		s.logger.Debug("No trades to process")

		return nil, nil
	}

	notifIndex, err := s.fetchNotificationIndex(ctx, trades)
	if err != nil {
		return nil, err
	}

	if err := s.writeRawData(trades, notifIndex); err != nil {
		return nil, err
	}

	processed := curateTrades(trades, notifIndex)
	if err := s.writeCurated(processed); err != nil {
		return nil, err
	}

	if err := s.cleanupMongo(ctx, trades, notifIndex); err != nil {
		return nil, err
	}

	return processed, nil
}

func (s *Service) fetchTrades(ctx context.Context) ([]models.BinanceTradeData, error) {
	limit := int64(s.cfg.BatchSize)
	sortField := "TradeTime"
	sortDir := repository.SortDirection(1)

	return s.tradeRepo.Find(ctx, &repository.BinanceTradeFilter{
		Limit:      &limit,
		SortField1: &sortField,
		SortDir1:   &sortDir,
	})
}

func (s *Service) fetchNotificationIndex(ctx context.Context, trades []models.BinanceTradeData) (map[repository.NotificationKey]*notificationMatch, error) {
	keys := uniqueNotificationKeys(trades)
	idx := make(map[repository.NotificationKey]*notificationMatch, len(keys))

	for _, key := range keys {
		symbol := key.Symbol
		eventTime := key.EventTime
		limit := int64(1)

		notifications, err := s.notifRepo.Find(ctx, &repository.NotificationFilter{
			Symbol:    &symbol,
			EventTime: &eventTime,
			Limit:     &limit,
		})
		if err != nil {
			return nil, err
		}

		if len(notifications) == 0 {
			continue
		}

		n := notifications[0]
		idx[repository.NotificationKey{Symbol: n.Symbol, EventTime: n.EventTime}] = &notificationMatch{data: n}
	}

	return idx, nil
}

func (s *Service) hasMoreTrades(ctx context.Context) (bool, error) {
	limit := int64(1)

	trades, err := s.tradeRepo.Find(ctx, &repository.BinanceTradeFilter{Limit: &limit})
	if err != nil {
		return false, err
	}

	return len(trades) > 0, nil
}

func (s *Service) writeRawData(trades []models.BinanceTradeData, notifIdx map[repository.NotificationKey]*notificationMatch) error {
	if err := s.writeRawTrades(trades); err != nil {
		return err
	}

	return s.writeRawNotifications(notifIdx)
}

func (s *Service) writeRawTrades(trades []models.BinanceTradeData) error {
	if len(trades) == 0 {
		return nil
	}

	partitions := make(map[string][]models.BinanceTradeData)

	for _, trade := range trades {
		partitionKey := buildRawTradePartitionKey(s.cfg.RawTradesPath, trade.TradeTime)
		partitions[partitionKey] = append(partitions[partitionKey], trade)
	}

	for partitionKey, batch := range partitions {
		payload, err := buildJSONLPayloadFromTrades(batch)
		if err != nil {
			return err
		}

		objectName := buildRawObjectPath(partitionKey)
		if err := s.putJSONLGzip(objectName, payload); err != nil {
			return err
		}

		s.logger.Debug("Wrote raw trades batch",
			slog.String("partition", partitionKey),
			slog.Int("count", len(batch)))
	}

	return nil
}

func (s *Service) writeRawNotifications(notifIdx map[repository.NotificationKey]*notificationMatch) error {
	if len(notifIdx) == 0 {
		return nil
	}

	partitions := make(map[string][]models.NotificationData)

	for _, match := range notifIdx {
		notif := match.data

		parsedTime, err := time.Parse(time.RFC3339Nano, notif.EventTime)
		if err != nil {
			parsedTime = time.Now().UTC()
		}

		partitionKey := buildRawNotificationPartitionKey(s.cfg.RawNotifsPath, parsedTime)
		partitions[partitionKey] = append(partitions[partitionKey], notif)
	}

	for partitionKey, batch := range partitions {
		payload, err := buildJSONLPayloadFromNotifications(batch)
		if err != nil {
			return err
		}

		objectName := buildRawObjectPath(partitionKey)
		if err := s.putJSONLGzip(objectName, payload); err != nil {
			return err
		}

		s.logger.Debug("Wrote raw notifications batch",
			slog.String("partition", partitionKey),
			slog.Int("count", len(batch)))
	}

	return nil
}

func (s *Service) putJSONLGzip(objectName string, payload []byte) error {
	compressed, err := compressPayloadToGzip(payload)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(compressed)

	_, err = s.minioClient.Insert(s.cfg.BucketName, objectName, reader, int64(len(compressed)), newJSONLGzipOptions())

	return err
}

func (s *Service) writeCurated(processed []models.ProcessedTrade) error {
	if len(processed) == 0 {
		return nil
	}

	partitions := partitionCuratedTrades(processed, s.cfg.CuratedPath)

	for prefix, rows := range partitions {
		payload, err := writeCuratedParquet(rows)
		if err != nil {
			return err
		}

		objectName := generateCuratedObjectName(prefix)
		reader := bytes.NewReader(payload)

		if _, err := s.minioClient.Insert(s.cfg.BucketName, objectName, reader, int64(len(payload)), newParquetPutOptions()); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) cleanupMongo(ctx context.Context, trades []models.BinanceTradeData, notifIdx map[repository.NotificationKey]*notificationMatch) error {
	ids := extractTradeIDs(trades)
	if _, err := s.tradeRepo.DeleteByIDs(ctx, ids); err != nil {
		return err
	}

	notifKeys := extractNotificationKeys(notifIdx)
	if _, err := s.notifRepo.DeleteByKeys(ctx, notifKeys); err != nil {
		return err
	}

	return nil
}

func (s *Service) runAnalytics(processed []models.ProcessedTrade) error {
	if len(processed) == 0 {
		return nil
	}

	engine := analytics.NewEngine(analytics.Config{AnalyticsPath: s.cfg.AnalyticsPath}, &analytics.MinioUploader{
		BucketName: s.cfg.BucketName,
		Client:     s.minioClient,
	})

	return engine.Process(processed)
}
