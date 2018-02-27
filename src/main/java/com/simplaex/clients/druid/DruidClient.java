package com.simplaex.clients.druid;

import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import com.simplaex.bedrock.Promise;
import io.druid.query.Query;
import io.druid.query.QueryPlus;

import java.time.Duration;
import java.util.List;

public interface DruidClient extends AutoCloseable {

  @FunctionalInterface
  interface EventEmitter extends Emitter {

    @Override
    default void start() {
    }

    void emit(Event var1);

    @Override
    default void flush() {
    }

    @Override
    default void close() {
    }

  }

  static DruidClient create(final String hostname, final int port) {
    return create(DruidClientConfig.builder().host(hostname).port(port).build());
  }

  static DruidClient create(final String hostname, final int port, final EventEmitter eventEmitter) {
    return new DruidClientImpl(DruidClientConfig.builder().host(hostname).port(port).eventEmitter(eventEmitter).build());
  }

  static DruidClient create(final DruidClientConfig config) {
    return new DruidClientImpl(config);
  }

  default <T> DruidResult<T> run(final Query<T> query) {
    return run(QueryPlus.wrap(query));
  }

  <T> DruidResult<T> run(QueryPlus<T> queryPlus);

  void cancel(DruidResult<?> druidResult);

  default <T> Promise<List<T>> run(final Query<T> query, final Duration timeout) {
    return run(QueryPlus.wrap(query), timeout);
  }

  <T> Promise<List<T>> run(QueryPlus<T> queryPlus, Duration timeout);

}
