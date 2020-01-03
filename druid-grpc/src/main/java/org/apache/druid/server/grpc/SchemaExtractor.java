/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.grpc;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.server.grpc.common.QuerySchemas.MetricType;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaDimension;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaMetric;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Extract the output schema from a query to configure the gRPC writer.
 * <p>
 * As schemas will differ in the number of metrics/dimensions only there is no need for a more generic approach such
 * as Avro. In addition we already use protobufs and piling Avro on top feels over-engineered.
 */
public final class SchemaExtractor
{
  public QuerySchema extract(GroupByQuery query)
  {
    final boolean hasTimeDimension = query.getResultRowHasTimestamp();
    final int baseIndex = hasTimeDimension ? 1 : 0;
    return new QuerySchema(
      hasTimeDimension,
      extractDimensions(baseIndex, query),
      extractMetrics(baseIndex + query.getDimensions().size(), query));
  }

  // todo append query.getPostAggregatorSpecs
  private List<QuerySchemaMetric> extractMetrics(int baseIndex, GroupByQuery query)
  {
    return IntStream
      .range(0, query.getAggregatorSpecs().size())
      .mapToObj(index -> {
        final AggregatorFactory af = query.getAggregatorSpecs().get(index);
        return new QuerySchemaMetric(baseIndex + index, af.getName(), MetricType.determineType(af.getTypeName()));
      })
      .collect(Collectors.toList());
  }

  private List<QuerySchemaDimension> extractDimensions(int baseIndex, GroupByQuery query)
  {
    return IntStream
      .range(0, query.getDimensions().size())
      .mapToObj(index -> new QuerySchemaDimension(baseIndex + index, query.getDimensions().get(index).getOutputName()))
      .collect(Collectors.toList());
  }
}
