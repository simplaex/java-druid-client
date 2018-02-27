package com.simplaex.clients.druid;

import lombok.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Builder
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class DruidClientConfig {

  @Getter
  private final String host;
  private final Integer port;

  private final Supplier<ExecutorService> executorServiceFactory;
  private final ExecutorService executorService;

  private final DruidClient.EventEmitter eventEmitter;

  public ExecutorService getExecutorService() {
    if (executorService == null) {
      if (executorServiceFactory == null) {
        return Executors.newWorkStealingPool();
      }
      return executorServiceFactory.get();
    }
    return executorService;
  }

  public DruidClient.EventEmitter getEventEmitter() {
    if (eventEmitter == null) {
      return __ -> {
      };
    }
    return eventEmitter;
  }

  public int getPort() {
    return port != null && port > 0 ? port : 8080;
  }

}
