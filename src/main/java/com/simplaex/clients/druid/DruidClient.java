package com.simplaex.clients.druid;

import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.core.Event;
import io.druid.query.Query;
import io.druid.query.QueryPlus;

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
    return create(hostname, port, __ -> {
    });
  }

  static DruidClient create(final String hostname, final int port, final EventEmitter eventEmitter) {
    return new DruidClientImpl(hostname, port, eventEmitter);
  }

  default <T> DruidResult<T> run(Query<T> query) {
    return run(QueryPlus.wrap(query));
  }

  <T> DruidResult<T> run(QueryPlus<T> queryPlus);

  void cancel(DruidResult<?> druidResult);

}
