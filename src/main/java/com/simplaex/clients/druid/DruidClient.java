package com.simplaex.clients.druid;

import com.simplaex.bedrock.Promise;

import io.druid.java.util.emitter.core.Emitter;
import io.druid.java.util.emitter.core.Event;
import io.druid.query.Query;
import io.druid.query.QueryPlus;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.List;

public interface DruidClient extends AutoCloseable {

  @FunctionalInterface
  interface EventEmitter extends Emitter {

    @Override
    default void start() {
    }

    void emit(Event event);

    @Override
    default void flush() {
    }

    @Override
    default void close() {
    }

  }

  @Nonnull
  static DruidClient create(final @Nonnull String hostname, final @Nonnegative int port) {
    return create(DruidClientConfig.builder().host(hostname).port(port).build());
  }

  @Nonnull
  static DruidClient create(final @Nonnull String hostname, final @Nonnegative int port, final @Nonnull EventEmitter eventEmitter) {
    return new DruidClientImpl(DruidClientConfig.builder().host(hostname).port(port).eventEmitter(eventEmitter).build());
  }

  @Nonnull
  static DruidClient create(final @Nonnull DruidClientConfig config) {
    return new DruidClientImpl(config);
  }

  @Nonnull
  default <T> DruidResult<T> run(final @Nonnull Query<T> query) {
    return run(QueryPlus.wrap(query));
  }

  @Nonnull
  <T> DruidResult<T> run(@Nonnull QueryPlus<T> queryPlus);

  void cancel(@Nonnull DruidResult<?> druidResult);

  @Nonnull
  default <T> Promise<List<T>> run(final @Nonnull Query<T> query, final @Nullable Duration timeout) {
    return run(QueryPlus.wrap(query), timeout);
  }

  <T> Promise<List<T>> run(@Nonnull QueryPlus<T> queryPlus, @Nullable Duration timeout);

}
