// This file and its contents are licensed under the Apache License 2.0.
// Please see the included NOTICE for copyright information and
// LICENSE for a copy of the license.

package ingestor

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	uber_atomic "go.uber.org/atomic"

	"github.com/timescale/promscale/pkg/log"
	"github.com/timescale/promscale/pkg/pgmodel/cache"
	"github.com/timescale/promscale/pkg/pgmodel/metrics"
	"github.com/timescale/promscale/pkg/pgmodel/model"
	"github.com/timescale/promscale/pkg/pgxconn"
	"github.com/timescale/promscale/pkg/tracer"
	tput "github.com/timescale/promscale/pkg/util/throughput"
)

const (
	finalizeMetricCreation    = "CALL _prom_catalog.finalize_metric_creation()"
	getEpochSQL               = "SELECT current_epoch, delete_epoch FROM _prom_catalog.ids_epoch LIMIT 1"
	getStaleSeriesIDsArraySQL = "SELECT ARRAY_AGG(id) FROM _prom_catalog.series WHERE delete_epoch IS NOT NULL"
)

var ErrDispatcherClosed = fmt.Errorf("dispatcher is closed")

// pgxDispatcher redirects incoming samples to the appropriate metricBatcher
// corresponding to the metric in the sample.
type pgxDispatcher struct {
	conn                   pgxconn.PgxConn
	metricTableNames       cache.MetricCache
	scache                 cache.SeriesCache
	invertedLabelsCache    *cache.InvertedLabelsCache
	exemplarKeyPosCache    cache.PositionCache
	batchers               sync.Map
	completeMetricCreation chan struct{}
	asyncAcks              bool
	copierReadRequestCh    chan<- readRequest
	seriesEpochRefresh     *time.Ticker
	doneChannel            chan struct{}
	closed                 *uber_atomic.Bool
	doneWG                 sync.WaitGroup
}

var _ model.Dispatcher = &pgxDispatcher{}

func newPgxDispatcher(conn pgxconn.PgxConn, mCache cache.MetricCache, scache cache.SeriesCache, eCache cache.PositionCache, cfg *Cfg) (*pgxDispatcher, error) {
	numCopiers := cfg.NumCopiers
	if numCopiers < 1 {
		log.Warn("msg", "num copiers less than 1, setting to 1")
		numCopiers = 1
	}

	// the copier read request channel retains the queue order between metrics
	maxMetrics := 10000
	copierReadRequestCh := make(chan readRequest, maxMetrics)

	metrics.IngestorChannelCap.With(prometheus.Labels{"type": "metric", "subsystem": "copier", "kind": "sample"}).Set(float64(cap(copierReadRequestCh)))
	metrics.RegisterCopierChannelLenMetric(func() float64 { return float64(len(copierReadRequestCh)) })

	if cfg.IgnoreCompressedChunks {
		// Handle decompression to not decompress anything.
		handleDecompression = skipDecompression
	}

	if err := model.RegisterCustomPgTypes(conn); err != nil {
		return nil, fmt.Errorf("registering custom pg types: %w", err)
	}

	labelArrayOID := model.GetCustomTypeOID(model.LabelArray)
	labelsCache, err := cache.NewInvertedLabelsCache(cfg.InvertedLabelsCacheSize)
	if err != nil {
		return nil, err
	}
	sw := NewSeriesWriter(conn, labelArrayOID, labelsCache, scache)
	elf := NewExamplarLabelFormatter(conn, eCache)

	for i := 0; i < numCopiers; i++ {
		go runCopier(conn, copierReadRequestCh, sw, elf)
	}

	inserter := &pgxDispatcher{
		conn:                   conn,
		metricTableNames:       mCache,
		scache:                 scache,
		invertedLabelsCache:    labelsCache,
		exemplarKeyPosCache:    eCache,
		completeMetricCreation: make(chan struct{}, 1),
		asyncAcks:              cfg.MetricsAsyncAcks,
		copierReadRequestCh:    copierReadRequestCh,
		// set to run at half our deletion interval
		// TODO: actually run this based on the deletion interval set in the DB.
		seriesEpochRefresh: time.NewTicker(30 * time.Minute),
		doneChannel:        make(chan struct{}),
		closed:             uber_atomic.NewBool(false),
	}
	inserter.closed.Store(false)
	runBatchWatcher(inserter.doneChannel)

	//on startup run a completeMetricCreation to recover any potentially
	//incomplete metric
	if err := inserter.CompleteMetricCreation(context.Background()); err != nil {
		return nil, err
	}

	go inserter.runCompleteMetricCreationWorker()

	if !cfg.DisableEpochSync {
		inserter.doneWG.Add(1)
		go func() {
			defer inserter.doneWG.Done()
			inserter.runSeriesEpochSync()
		}()
	}
	return inserter, nil
}

func (p *pgxDispatcher) runCompleteMetricCreationWorker() {
	for range p.completeMetricCreation {
		err := p.CompleteMetricCreation(context.Background())
		if err != nil {
			log.Warn("msg", "Got an error finalizing metric", "err", err)
		}
	}
}

func (p *pgxDispatcher) runSeriesEpochSync() {
	p.refreshSeriesEpoch()
	for {
		select {
		case <-p.seriesEpochRefresh.C:
			p.refreshSeriesEpoch()
		case <-p.doneChannel:
			return
		}
	}
}

func (p *pgxDispatcher) refreshSeriesEpoch() {
	log.Info("msg", "Refreshing series cache epoch")
	var dbCurrentEpochRaw, dbDeleteEpochRaw int64
	row := p.conn.QueryRow(context.Background(), getEpochSQL)
	err := row.Scan(&dbCurrentEpochRaw, &dbDeleteEpochRaw)
	dbCurrentEpoch, dbDeleteEpoch := model.NewSeriesEpoch(dbCurrentEpochRaw), model.NewSeriesEpoch(dbDeleteEpochRaw)

	cacheCurrentEpoch := p.scache.CacheEpoch()

	if err != nil {
		log.Info("msg", "An error occurred refreshing, will reset series and inverted labels caches")
		// Trash the cache just in case an epoch change occurred, seems safer
		p.scache.Reset()
		// Also trash the inverted labels cache, which can also be invalidated when the series cache is
		p.invertedLabelsCache.Reset()
		return
	}
	if dbDeleteEpoch.AfterEq(cacheCurrentEpoch) {
		// The current cache epoch has been overtaken by the database' delete_epoch.
		// The only way to recover from this situation is to reset our caches and let them
		// repopulate.
		log.Warn("msg", "Cache epoch was overtaken by the database's delete epoch", "cache_epoch", cacheCurrentEpoch, "delete_epoch", dbDeleteEpoch)
		p.scache.Reset()
		p.invertedLabelsCache.Reset()
		return
	} else {
		start := time.Now()
		staleSeriesIds, err := GetStaleSeriesIDs(p.conn)
		if err != nil {
			log.Error("msg", "error getting series ids, resetting series cache", "err", err.Error())
			p.scache.Reset()
			p.invertedLabelsCache.Reset()
			return
		}
		log.Info("msg", "epoch change noticed, fetched stale series from db", "count", len(staleSeriesIds), "duration", time.Since(start))
		start = time.Now()
		evictCount := p.scache.EvictSeriesById(staleSeriesIds)
		log.Info("msg", "removed stale series", "count", evictCount, "duration", time.Since(start)) // Before merging in master, change the level to Debug.
		// TODO (james): possibly describe a little better why we're killing this cache.
		// Trash the inverted labels cache for now.
		p.invertedLabelsCache.Reset()
	}
	p.scache.SetCacheEpochFromRefresh(dbCurrentEpoch)
}

func GetStaleSeriesIDs(conn pgxconn.PgxConn) ([]model.SeriesID, error) {
	// TODO (james): unclear what the ideal initial size for this array is
	staleSeriesIDs := make([]int64, 0, 100000)
	err := conn.QueryRow(context.Background(), getStaleSeriesIDsArraySQL).Scan(&staleSeriesIDs)
	if err != nil {
		return nil, fmt.Errorf("error getting stale series ids from db: %w", err)
	}
	staleSeriesIdsAsSeriesId := make([]model.SeriesID, len(staleSeriesIDs))
	for i := range staleSeriesIDs {
		staleSeriesIdsAsSeriesId[i] = model.SeriesID(staleSeriesIDs[i])
	}
	return staleSeriesIdsAsSeriesId, nil
}

func (p *pgxDispatcher) CompleteMetricCreation(ctx context.Context) error {
	if p.closed.Load() {
		return ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-complete-metric-creation")
	defer span.End()
	_, err := p.conn.Exec(
		context.Background(),
		finalizeMetricCreation,
	)
	return err
}

func (p *pgxDispatcher) Close() {
	if p.closed.Load() {
		return
	}
	p.closed.Store(true)
	close(p.completeMetricCreation)
	p.batchers.Range(func(key, value interface{}) bool {
		close(value.(chan *insertDataRequest))
		return true
	})

	close(p.copierReadRequestCh)
	close(p.doneChannel)
	p.doneWG.Wait()
}

// InsertTs inserts a batch of data into the database.
// The data should be grouped by metric name.
// returns the number of rows we intended to insert (_not_ how many were
// actually inserted) and any error.
// Though we may insert data to multiple tables concurrently, if asyncAcks is
// unset this function will wait until _all_ the insert attempts have completed.
func (p *pgxDispatcher) InsertTs(ctx context.Context, dataTS model.Data) (uint64, error) {
	if p.closed.Load() {
		return 0, ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-insert-ts")
	defer span.End()
	var (
		numRows      uint64
		maxt         int64
		rows         = dataTS.Rows
		workFinished = new(sync.WaitGroup)
	)
	workFinished.Add(len(rows))
	// we only allocate enough space for a single error message here as we only
	// report one error back upstream. The inserter should not block on this
	// channel, but only insert if it's empty, anything else can deadlock.
	errChan := make(chan error, 1)
	for metricName, data := range rows {
		for _, insertable := range data {
			numRows += uint64(insertable.Count())
			ts := insertable.MaxTs()
			if maxt < ts {
				maxt = ts
			}
		}
		p.getMetricBatcher(metricName) <- &insertDataRequest{spanCtx: span.SpanContext(), metric: metricName, data: data, finished: workFinished, errChan: errChan}
	}
	span.SetAttributes(attribute.Int64("num_rows", int64(numRows)))
	span.SetAttributes(attribute.Int("num_metrics", len(rows)))
	reportIncomingBatch(numRows)
	reportOutgoing := func() {
		reportOutgoingBatch(numRows)
		reportBatchProcessingTime(dataTS.ReceivedTime)
	}

	var err error
	if !p.asyncAcks {
		workFinished.Wait()
		reportOutgoing()
		select {
		case err = <-errChan:
		default:
		}
		reportMetricsTelemetry(maxt, numRows, 0)
		close(errChan)
	} else {
		go func() {
			workFinished.Wait()
			reportOutgoing()
			select {
			case err = <-errChan:
			default:
			}
			close(errChan)
			if err != nil {
				log.Error("msg", fmt.Sprintf("error on async send, dropping %d datapoints", numRows), "err", err)
			}
			reportMetricsTelemetry(maxt, numRows, 0)
		}()
	}

	return numRows, err
}

func (p *pgxDispatcher) InsertMetadata(ctx context.Context, metadata []model.Metadata) (uint64, error) {
	if p.closed.Load() {
		return 0, ErrDispatcherClosed
	}
	_, span := tracer.Default().Start(ctx, "dispatcher-insert-metadata")
	defer span.End()
	totalRows := uint64(len(metadata))
	insertedRows, err := insertMetadata(p.conn, metadata)
	if err != nil {
		return insertedRows, err
	}
	reportMetricsTelemetry(0, 0, insertedRows)
	if totalRows != insertedRows {
		return insertedRows, fmt.Errorf("failed to insert all metadata: inserted %d rows out of %d rows in total", insertedRows, totalRows)
	}
	return insertedRows, nil
}

func reportMetricsTelemetry(maxTs int64, numSamples, numMetadata uint64) {
	tput.ReportMetricsProcessed(maxTs, numSamples, numMetadata)

	// Max_sent_timestamp stats.
	if maxTs < atomic.LoadInt64(&metrics.MaxSentTs) {
		return
	}
	atomic.StoreInt64(&metrics.MaxSentTs, maxTs)
	metrics.IngestorMaxSentTimestamp.With(prometheus.Labels{"type": "metric"}).Set(float64(maxTs))
}

// Get the handler for a given metric name, creating a new one if none exists
func (p *pgxDispatcher) getMetricBatcher(metric string) chan<- *insertDataRequest {
	batcher, ok := p.batchers.Load(metric)
	if !ok {
		// The ordering is important here: we need to ensure that every call
		// to getMetricInserter() returns the same inserter. Therefore, we can
		// only start up the inserter routine if we know that we won the race
		// to create the inserter, anything else will leave a zombie inserter
		// lying around.
		c := make(chan *insertDataRequest, metrics.MetricBatcherChannelCap)
		actual, old := p.batchers.LoadOrStore(metric, c)
		batcher = actual
		if !old {
			go runMetricBatcher(p.conn, c, metric, p.completeMetricCreation, p.metricTableNames, p.copierReadRequestCh)
		}
	}
	ch := batcher.(chan *insertDataRequest)
	metrics.IngestorChannelLenBatcher.Set(float64(len(ch)))
	return ch
}

type insertDataRequest struct {
	spanCtx  trace.SpanContext
	metric   string
	finished *sync.WaitGroup
	data     []model.Insertable
	errChan  chan error
}

func (idr *insertDataRequest) reportResult(err error) {
	if err != nil {
		select {
		case idr.errChan <- err:
		default:
		}
	}
	idr.finished.Done()
}
