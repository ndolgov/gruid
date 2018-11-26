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

import org.apache.druid.query.Query;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.server.grpc.common.QuerySchemas.MetricType;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchemaMetric;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Extract the output schema from a query to configure the gRPC writer.
 * <p>
 * As schemas will differ in the number of metrics/dimensions only there is no need for a more generic approach such
 * as Avro. In addition we already use protobufs and piling Avro on top feels over-engineered.
 */
public final class SchemaExtractor
{
  public QuerySchema extract(Query query)
  {
    if (query instanceof GroupByQuery) {
      final GroupByQuery gbq = (GroupByQuery) query;
      return new QuerySchema(extractDimensions(gbq), extractMetrics(gbq));
    }

    throw new IllegalArgumentException("Unsupported query type " + query);
  }

  // todo append query.getPostAggregatorSpecs
  private List<QuerySchemaMetric> extractMetrics(GroupByQuery query)
  {
    return query.getAggregatorSpecs().stream()
      .map(af -> new QuerySchemaMetric(af.getName(), MetricType.determineType(af.getTypeName())))
      .collect(Collectors.toList());
  }

  private List<String> extractDimensions(GroupByQuery query)
  {
    return query.getDimensions().stream().map(DimensionSpec::getOutputName).collect(Collectors.toList());
  }
}
