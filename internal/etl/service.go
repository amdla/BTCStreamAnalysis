package etl

import (
	"app/internal/analytics"
	"app/internal/minio"
	"app/internal/models"
	"app/internal/mongo"
	"app/internal/repository"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	minioSDK "github.com/minio/minio-go/v7"
	"github.com/xitongsys/parquet-go-source/buffer"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
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

func InitializeLogger(debug bool) *slog.Logger {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}

	return slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
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

	runOnce := func() {
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

	runOnce()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("ETL service stopping")
			return nil
		case <-ticker.C:
			runOnce()
		}
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

type notificationMatch struct {
	data     models.NotificationData
	consumed bool
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

	processed := s.curateTrades(trades, notifIndex)
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

	trades, err := s.tradeRepo.Find(ctx, &repository.BinanceTradeFilter{
		Limit:      &limit,
		SortField1: &sortField,
		SortDir1:   &sortDir,
	})
	if err != nil {
		return nil, err
	}

	return trades, nil
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

	// Group trades by hour partition
	partitions := make(map[string][]models.BinanceTradeData)
	for _, trade := range trades {
		partitionKey := buildRawTradePartitionKey(s.cfg.RawTradesPath, trade.TradeTime)
		partitions[partitionKey] = append(partitions[partitionKey], trade)
	}

	// Write one file per partition containing all trades in that partition
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

	// Group notifications by day partition
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

	// Write one file per partition containing all notifications in that partition
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

func buildRawTradePartitionKey(prefix string, tradeTime time.Time) string {
	utc := tradeTime.UTC()
	// Partition by hour for efficient batching
	return fmt.Sprintf("%s/%04d/%02d/%02d/%02d", prefix, utc.Year(), utc.Month(), utc.Day(), utc.Hour())
}

func buildRawNotificationPartitionKey(prefix string, eventTime time.Time) string {
	utc := eventTime.UTC()
	// Partition by day for efficient batching
	return fmt.Sprintf("%s/%04d/%02d/%02d", prefix, utc.Year(), utc.Month(), utc.Day())
}

func buildRawObjectPath(partitionKey string) string {
	// Use single timestamp per batch write to avoid file collisions
	return fmt.Sprintf("%s/%d.jsonl.gz", partitionKey, time.Now().UTC().UnixNano())
}

func buildJSONLPayloadFromTrades(trades []models.BinanceTradeData) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	for i, trade := range trades {
		if err := encoder.Encode(trade); err != nil {
			return nil, fmt.Errorf("encode trade jsonl: %w", err)
		}
		if i < len(trades)-1 {
			buf.Truncate(buf.Len() - 1)
		}
	}

	return buf.Bytes(), nil
}

func buildJSONLPayloadFromNotifications(notifs []models.NotificationData) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)

	for i, notif := range notifs {
		if err := encoder.Encode(notif); err != nil {
			return nil, fmt.Errorf("encode notification jsonl: %w", err)
		}
		if i < len(notifs)-1 {
			buf.Truncate(buf.Len() - 1)
		}
	}

	return buf.Bytes(), nil
}

func (s *Service) putJSONLGzip(objectName string, payload []byte) error {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		return err
	}
	if err := gz.Close(); err != nil {
		return err
	}

	reader := bytes.NewReader(buf.Bytes())
	opts := minioSDK.PutObjectOptions{
		ContentType:     "application/jsonl",
		ContentEncoding: "gzip",
	}

	_, err := s.minioClient.Insert(s.cfg.BucketName, objectName, reader, int64(buf.Len()), opts)
	return err
}

func (s *Service) putJSONL(objectName string, payload []byte) error {
	return s.putJSONLGzip(objectName, payload)
}

func (s *Service) curateTrades(trades []models.BinanceTradeData, notifIdx map[repository.NotificationKey]*notificationMatch) []models.ProcessedTrade {
	processed := make([]models.ProcessedTrade, len(trades))
	for i, trade := range trades {
		triggered := hasNotificationForTrade(notifIdx, trade)
		processed[i] = (models.EnrichedTrade{BinanceTradeData: trade, TriggeredNotification: triggered}).ToProcessedTrade()
	}
	return processed
}

func (s *Service) writeCurated(processed []models.ProcessedTrade) error {
	if len(processed) == 0 {
		return nil
	}

	partitions := make(map[string][]*models.CuratedTradeParquet)
	for _, trade := range processed {
		parquetRow := models.NewCuratedTradeParquet(trade)
		objectPrefix := buildCuratedObjectPrefix(s.cfg.CuratedPath, parquetRow.Symbol, parquetRow.Date)
		partitions[objectPrefix] = append(partitions[objectPrefix], parquetRow)
	}

	for prefix, rows := range partitions {
		objectName := fmt.Sprintf("%s/%d.parquet", prefix, time.Now().UTC().UnixNano())
		if err := s.putParquet(objectName, rows); err != nil {
			return err
		}
	}

	return nil
}

func buildCuratedObjectPrefix(base, symbol, date string) string {
	return fmt.Sprintf("%s/symbol=%s/date=%s", strings.TrimSuffix(base, "/"), symbol, date)
}

func (s *Service) putParquet(objectName string, rows []*models.CuratedTradeParquet) error {
	buf := buffer.NewBufferFile()
	pw, err := writer.NewParquetWriter(buf, new(models.CuratedTradeParquet), 1)
	if err != nil {
		return err
	}
	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, row := range rows {
		if err := pw.Write(row); err != nil {
			return err
		}
	}

	if err := pw.WriteStop(); err != nil {
		return err
	}

	payload := buf.Bytes()
	reader := bytes.NewReader(payload)
	opts := minioSDK.PutObjectOptions{ContentType: "application/octet-stream"}

	_, err = s.minioClient.Insert(s.cfg.BucketName, objectName, reader, int64(len(payload)), opts)
	return err
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

func (s *Service) hasMoreTrades(ctx context.Context) (bool, error) {
	limit := int64(1)
	trades, err := s.tradeRepo.Find(ctx, &repository.BinanceTradeFilter{Limit: &limit})
	if err != nil {
		return false, err
	}

	return len(trades) > 0, nil
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

func (s *Service) deleteTrades(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	_, err := s.tradeRepo.DeleteByIDs(ctx, ids)
	return err
}

func (s *Service) deleteNotifications(ctx context.Context, keys []repository.NotificationKey) error {
	if len(keys) == 0 {
		return nil
	}

	_, err := s.notifRepo.DeleteByKeys(ctx, keys)
	return err
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
