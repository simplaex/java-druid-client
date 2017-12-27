package com.simplaex.clients.druid;

import io.druid.query.Query;
import io.druid.query.QueryPlus;

public interface DruidClient extends AutoCloseable {

  static DruidClient create(final String hostname, final int port) {
    return create("http", hostname, port);
  }

  static DruidClient create(final String protocolScheme, final String hostname, final int port) {
    switch (protocolScheme) {
      case "http":
      case "https":
        break;
      default:
        throw new IllegalArgumentException("Unsupported protocol scheme: " + protocolScheme);
    }
    return new DruidClientImpl(protocolScheme, hostname, port);
  }

  default <T> DruidResult<T> run(Query<T> query) {
    return run(QueryPlus.wrap(query));
  }

  <T> DruidResult<T> run(QueryPlus<T> queryPlus);

  void cancel(DruidResult<?> druidResult);

}
