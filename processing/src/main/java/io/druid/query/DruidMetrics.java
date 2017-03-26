/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;

/**
 */
public class DruidMetrics
{
  public final static String DATASOURCE = "dataSource";
  public final static String TYPE = "type";
  public final static String INTERVAL = "interval";
  public final static String ID = "id";
  public final static String TASK_ID = "taskId";
  public final static String STATUS = "status";

  // task metrics
  public final static String TASK_TYPE = "taskType";
  public final static String TASK_STATUS = "taskStatus";

  public final static String SERVER = "server";
  public final static String TIER = "tier";

  public static int findNumComplexAggs(List<AggregatorFactory> aggs)
  {
    int retVal = 0;
    for (AggregatorFactory agg : aggs) {
      // This needs to change when we have support column types better
      if (!agg.getTypeName().equals("float") && !agg.getTypeName().equals("long")) {
        retVal++;
      }
    }
    return retVal;
  }

//<<<<<<< HEAD
//  public static <T> ServiceMetricEvent.Builder makePartialQueryTimeMetric(Query<T> query)
//  {
//    return new ServiceMetricEvent.Builder()
//        .setDimension(DATASOURCE, DataSourceUtil.getMetricName(query.getDataSources()))
//        .setDimension(TYPE, query.getType())
////        .setDimension(
////            INTERVAL,
////            Lists.transform(
////                query.getIntervals(),
////                new Function<Interval, String>()
////                {
////                  @Override
////                  public String apply(Interval input)
////                  {
////                    return input.toString();
////                  }
////                }
////            ).toArray(new String[query.getIntervals().size()])
////        )
//        .setDimension("hasFilters", String.valueOf(query.hasFilters()))
////        .setDimension("duration", query.getDuration().toString())
//        .setDimension("duration", query.getTotalDuration().toString())
//        .setDimension(ID, Strings.nullToEmpty(query.getId()));
//  }
//
//  public static <T> ServiceMetricEvent.Builder makeQueryTimeMetric(
//=======
  public static <T> QueryMetrics<?> makeRequestMetrics(
      final GenericQueryMetricsFactory queryMetricsFactory,
//>>>>>>> a0f2cf05d5a3a7d71635d1f54dd0efdfe57e469a
      final QueryToolChest<T, Query<T>> toolChest,
      final Query<T> query,
      final String remoteAddr
  ) throws JsonProcessingException
  {
    QueryMetrics<? super Query<T>> queryMetrics;
    if (toolChest != null) {
      queryMetrics = toolChest.makeMetrics(query);
    } else {
      queryMetrics = queryMetricsFactory.makeMetrics(query);
    }
    queryMetrics.context(query);
    queryMetrics.remoteAddress(remoteAddr);
    return queryMetrics;
  }
}
