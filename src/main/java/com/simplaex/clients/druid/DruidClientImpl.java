package com.simplaex.clients.druid;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.simplaex.bedrock.Promise;
import io.druid.client.DirectDruidClient;
import io.druid.collections.BlockingPool;
import io.druid.collections.DefaultBlockingPool;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.StupidPool;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.service.ServiceEmitter;
import io.druid.java.util.http.client.HttpClient;
import io.druid.java.util.http.client.HttpClientConfig;
import io.druid.java.util.http.client.HttpClientInit;
import io.druid.query.*;
import io.druid.query.datasourcemetadata.DataSourceMetadataQuery;
import io.druid.query.datasourcemetadata.DataSourceQueryQueryToolChest;
import io.druid.query.groupby.*;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.query.groupby.strategy.GroupByStrategyV1;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanQueryConfig;
import io.druid.query.scan.ScanQueryQueryToolChest;
import io.druid.query.search.SearchQueryQueryToolChest;
import io.druid.query.search.SearchQuery;
import io.druid.query.search.SearchQueryConfig;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectQueryConfig;
import io.druid.query.select.SelectQueryQueryToolChest;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import io.druid.query.timeseries.DefaultTimeseriesQueryMetricsFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryMetricsFactory;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.topn.*;
import io.druid.server.QueryManager;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;

public final class DruidClientImpl implements DruidClient {

  private static final int BYTE_BUFFER_CAPACITY = 4096;
  private static final int MERGE_BUFFER_CAPACITY = 4096;
  private static final int MERGE_BUFFER_LIMIT = 100;
  private static final int SHUTDOWN_TIMEOUT_SECONDS = 60;

  private final QueryManager queryManager;

  private final ExecutorService executorService;

  private final DirectDruidClient druidClient;

  DruidClientImpl(final DruidClientConfig config) {
    final ServiceEmitter serviceEmitter = createServiceEmitter(config.getHost(), config.getEventEmitter());
    this.queryManager = new QueryManager();
    this.executorService = config.getExecutorService();
    this.druidClient = createDruidClient(config.getHost(), config.getPort(), queryManager, serviceEmitter, executorService,
      config.getObjectMapper());
  }

  private static DirectDruidClient createDruidClient(
    final String hostname,
    final int port,
    final QueryWatcher queryWatcher,
    final ServiceEmitter serviceEmitter,
    final ExecutorService executorService,
    final ObjectMapper objectMapper
  ) {
    final String host = String.format("%s:%d", hostname, port);
    return new DirectDruidClient(
      createQueryToolChestWarehouse(objectMapper, serviceEmitter, queryWatcher, executorService),
      queryWatcher,
      objectMapper,
      createHttpClient(),
      "http",
      host, serviceEmitter
    );
  }

  private static ServiceEmitter createServiceEmitter(final String host, final Emitter emitter) {
    return new ServiceEmitter(
      "druid",
      host,
      emitter
    );
  }

  private static QueryToolChestWarehouse createQueryToolChestWarehouse(
    final ObjectMapper objectMapper,
    final ServiceEmitter serviceEmitter,
    final QueryWatcher queryWatcher,
    final ExecutorService executorService
  ) {

    final Map<Class<? extends Query>, QueryToolChest> chestMap = new HashMap<>();

    // reusable things

    final GenericQueryMetricsFactory genericQueryMetricsFactory =
      new DefaultGenericQueryMetricsFactory(objectMapper);

    final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator =
      new IntervalChunkingQueryRunnerDecorator(executorService, queryWatcher, serviceEmitter);


    // datasource metadata queries

    final DataSourceQueryQueryToolChest dataSourceQueryQueryToolChest =
      new DataSourceQueryQueryToolChest(genericQueryMetricsFactory);

    chestMap.put(DataSourceMetadataQuery.class, dataSourceQueryQueryToolChest);


    // groupBy queries

    final GroupByQueryMetricsFactory groupByQueryMetricsFactory =
      new DefaultGroupByQueryMetricsFactory(objectMapper);

    @SuppressWarnings("Guava") final Supplier<GroupByQueryConfig> groupByQueryConfigSupplier =
      GroupByQueryConfig::new;

    final NonBlockingPool<ByteBuffer> v1StrategyByteBufferPool =
      new StupidPool<>("druid-groupby-strategy-v1-bytebuffer-pool", () -> ByteBuffer.allocate(BYTE_BUFFER_CAPACITY));

    final NonBlockingPool<ByteBuffer> queryEngineBufferPool =
      new StupidPool<>("druid-groupby-queryengine-bytebuffer-pool", () -> ByteBuffer.allocate(BYTE_BUFFER_CAPACITY));

    final GroupByQueryEngine groupByQueryEngine =
      new GroupByQueryEngine(groupByQueryConfigSupplier, queryEngineBufferPool);

    final GroupByStrategyV1 groupByStrategyV1 =
      new GroupByStrategyV1(
        groupByQueryConfigSupplier,
        groupByQueryEngine,
        queryWatcher,
        v1StrategyByteBufferPool
      );

    final DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig() {
      @Override
      public String getFormatString() {
        return "%s %s %s %s %s %s %s (I DON'T KNOW FIX ME)";
      }
    };

    final NonBlockingPool<ByteBuffer> v2StrategyByteBufferPool =
      new StupidPool<>(
        "druid-groupby-strategy-v2-bytebuffer-pool",
        () -> ByteBuffer.allocate(BYTE_BUFFER_CAPACITY)
      );

    final BlockingPool<ByteBuffer> mergeBufferPool =
      new DefaultBlockingPool<>(() -> ByteBuffer.allocate(MERGE_BUFFER_CAPACITY), MERGE_BUFFER_LIMIT);

    final GroupByStrategyV2 groupByStrategyV2 =
      new GroupByStrategyV2(
        druidProcessingConfig,
        groupByQueryConfigSupplier,
        v2StrategyByteBufferPool,
        mergeBufferPool,
        objectMapper,
        queryWatcher
      );

    final GroupByStrategySelector groupByStrategySelector =
      new GroupByStrategySelector(
        groupByQueryConfigSupplier,
        groupByStrategyV1,
        groupByStrategyV2
      );

    final GroupByQueryQueryToolChest groupByQueryQueryToolChest =
      new GroupByQueryQueryToolChest(
        groupByStrategySelector,
        intervalChunkingQueryRunnerDecorator,
        groupByQueryMetricsFactory
      );

    chestMap.put(GroupByQuery.class, groupByQueryQueryToolChest);

    // search queries

    final SearchQueryConfig searchQueryConfig =
      new SearchQueryConfig();

    final SearchQueryQueryToolChest searchQueryQueryToolChest =
      new SearchQueryQueryToolChest(
        searchQueryConfig,
        intervalChunkingQueryRunnerDecorator
      );

    chestMap.put(SearchQuery.class, searchQueryQueryToolChest);


    // segment metadata queries

    final SegmentMetadataQueryConfig segmentMetadataQueryConfig =
      new SegmentMetadataQueryConfig();

    final SegmentMetadataQueryQueryToolChest segementMetadataToolChest =
      new SegmentMetadataQueryQueryToolChest(
        segmentMetadataQueryConfig,
        genericQueryMetricsFactory
      );

    chestMap.put(SegmentMetadataQuery.class, segementMetadataToolChest);


    // select queries

    @SuppressWarnings("Guava") final Supplier<SelectQueryConfig> selectQueryConfigSupplier =
      () -> new SelectQueryConfig(null);

    final SelectQueryQueryToolChest selectQueryQueryToolChest =
      new SelectQueryQueryToolChest(
        objectMapper,
        intervalChunkingQueryRunnerDecorator,
        selectQueryConfigSupplier
      );

    chestMap.put(SelectQuery.class, selectQueryQueryToolChest);


    // timeboundary queries

    final TimeBoundaryQueryQueryToolChest timeBoundaryQueryQueryToolChest =
      new TimeBoundaryQueryQueryToolChest();

    chestMap.put(TimeBoundaryQuery.class, timeBoundaryQueryQueryToolChest);


    // timeseries queries

    final TimeseriesQueryMetricsFactory timeseriesMetricsFactory =
      new DefaultTimeseriesQueryMetricsFactory(objectMapper);

    final TimeseriesQueryQueryToolChest timeseriesToolChest =
      new TimeseriesQueryQueryToolChest(
        intervalChunkingQueryRunnerDecorator,
        timeseriesMetricsFactory
      );

    chestMap.put(TimeseriesQuery.class, timeseriesToolChest);


    // top n queries

    final TopNQueryConfig topNQueryConfig = new TopNQueryConfig();

    final TopNQueryMetricsFactory topNQueryMetricsFactory =
      new DefaultTopNQueryMetricsFactory(objectMapper);

    final TopNQueryQueryToolChest topNQueryQueryToolChest =
      new TopNQueryQueryToolChest(
        topNQueryConfig,
        intervalChunkingQueryRunnerDecorator,
        topNQueryMetricsFactory
      );

    chestMap.put(TopNQuery.class, topNQueryQueryToolChest);


    // scan queries

    final ScanQueryConfig scanQueryConfig = new ScanQueryConfig();

    final ScanQueryQueryToolChest scanToolChest =
      new ScanQueryQueryToolChest(
        scanQueryConfig,
        genericQueryMetricsFactory
      );

    chestMap.put(ScanQuery.class, scanToolChest);


    return new MapQueryToolChestWarehouse(chestMap);
  }

  private static HttpClient createHttpClient() {
    final HttpClientConfig httpClientConfig = HttpClientConfig.builder().build();
    final Lifecycle lifecycle = new Lifecycle();
    return HttpClientInit.createClient(
      httpClientConfig,
      lifecycle
    );
  }

  @Nonnull
  @Override
  public <T> DruidResult<T> run(@Nonnull final QueryPlus<T> queryPlus) {
    final Query<T> query = queryPlus.getQuery();
    final Query<T> queryWithId =
      query.getId() == null ? query.withId(UUID.randomUUID().toString()) : query;
    final long startTimeMillis = System.currentTimeMillis();
    final Map<String, Object> responseContext =
      DirectDruidClient.makeResponseContextForQuery(queryWithId, startTimeMillis);
    final QueryPlus<T> finalQuery = queryPlus.withQuery(queryWithId);
    //noinspection unchecked
    final Sequence<T> resultSequence =
      druidClient.run(finalQuery, responseContext);
    return new DruidResult<>(resultSequence, finalQuery);
  }

  @Nonnull
  @Override
  public <T> Promise<List<T>> run(@Nonnull final QueryPlus<T> queryPlus, @Nullable final Duration timeout) {
    final Promise<List<T>> promise = Promise.promise();
    if (timeout == null || timeout.isZero() || timeout.isNegative()) {
      executorService.submit(() -> {
        try {
          final DruidResult<T> result = run(queryPlus);
          final List<T> resultList = result.toList();
          promise.fulfill(resultList);
        } catch (final Exception exc) {
          promise.fail(exc);
        }
      });
    } else {
      final Future<List<T>> future = executorService.submit(() -> {
        final DruidResult<T> result = run(queryPlus);
        return result.toList();
      });
      executorService.submit(() -> {
        try {
          final List<T> resultList = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
          promise.fulfill(resultList);
        } catch (final Exception exc) {
          promise.fail(exc);
        }
      });
    }
    return promise;
  }

  @Override
  public void cancel(@Nonnull final DruidResult<?> druidResult) {
    queryManager.cancelQuery(druidResult.getQueryId());
  }

  @Override
  public void close() throws Exception {
    executorService.shutdown();
    executorService.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

}
