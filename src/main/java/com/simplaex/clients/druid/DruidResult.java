package com.simplaex.clients.druid;

import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.query.QueryPlus;
import lombok.Value;

import java.util.ArrayList;
import java.util.List;

@Value
public class DruidResult<T> {

  Sequence<T> sequence;

  QueryPlus<T> query;

  public List<T> toList() {
    final Yielder<ArrayList<T>> resultYielder = sequence.toYielder(
      new ArrayList<>(),
        new YieldingAccumulator<ArrayList<T>, T>() {
          @Override
          public ArrayList<T> accumulate(final ArrayList<T> accumulated, final T in) {
            accumulated.add(in);
            return accumulated;
          }
        });
    return resultYielder.get();
  }

  public String getQueryId() {
    return query.getQuery().getId();
  }
}
