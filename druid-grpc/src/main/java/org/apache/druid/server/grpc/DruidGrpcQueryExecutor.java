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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.grpc.stub.StreamObserver;
import org.apache.druid.data.input.Row;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.grpc.common.QuerySchemas.QuerySchema;
import org.apache.druid.server.security.AllowAllAuthenticator;

/**
 * Parse query JSON, execute the query, iterate the results.
 * <p>
 * todo consider Calcite/Avatica integration in the next phase
 */
public final class DruidGrpcQueryExecutor implements GrpcQueryExecutor
{
  private static final Logger logger = new Logger(DruidGrpcQueryExecutor.class);

  private final QueryLifecycleFactory queryLifecycleFactory;

  private final ObjectMapper jsonMapper;

  private final SchemaExtractor schemaExtractor = new SchemaExtractor();

  @Inject
  public DruidGrpcQueryExecutor(QueryLifecycleFactory queryLifecycleFactory, @Json ObjectMapper jsonMapper)
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public void execute(String queryJson, StreamObserver<GrpcRowBatch.RowBatchResponse> observer )
  {
    logger.info("Processing query: " + queryJson);

    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    try {
      final Query<Object> query = unmarshalQuery(queryJson);
      final QuerySchema schema = schemaExtractor.extract(query);
      final RowBatcher batcher = new RowBatcher(observer, new RowBatchContext(10, schema));

      final Sequence seq = queryLifecycle.runSimple(query, AllowAllAuthenticator.ALLOW_ALL_RESULT, null); // todo client ref?

      Yielder<Row> yielder = Yielders.each(seq); // todo <Row> is returned by GroupBy queries only
      try {
        while (!yielder.isDone()) {
          final Row row = yielder.get();
          batcher.onRow(row);
          yielder = yielder.next(null);
        }
        batcher.onComplete();
      }
      catch (Exception e) {
        try {
          yielder.close();
        }
        catch (Exception ex) {
          logger.warn(ex, "Failed to close yielder");
        }
        batcher.onError(e);
      }
    }
    catch (Exception e) {
      logger.error(e, "Failed to execute query:" + queryJson);
      observer.onError(e);
    }
  }

  private Query<Object> unmarshalQuery(String queryJson)
  {
    try {
      return jsonMapper.readValue(queryJson, Query.class);
    }
    catch (Exception e) {
      throw new IllegalArgumentException("Invalid query: " + queryJson, e);
    }
  }
}
