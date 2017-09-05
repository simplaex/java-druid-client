package com.simplaex.clients.druid;

import io.druid.query.Query;
import io.druid.query.QueryPlus;

public interface DruidClient extends AutoCloseable {

  static DruidClient create(final String hostname, final int port) {
    return new DruidClientImpl(hostname, port);
  }

  default <T> DruidResult<T> run(Query<T> query) {
    return run(QueryPlus.wrap(query));
  }

  <T> DruidResult<T> run(QueryPlus<T> queryPlus);

  void cancel(DruidResult<?> druidResult);

}
