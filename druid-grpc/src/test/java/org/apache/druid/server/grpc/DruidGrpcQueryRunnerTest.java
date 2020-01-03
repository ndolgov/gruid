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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.grpc.stub.StreamObserver;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.expression.LookupEnabledTestExprMacroTable;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.grpc.common.DictionaryEncoders;
import org.apache.druid.server.grpc.common.Marshallers;
import org.apache.druid.server.grpc.common.RowBatch;
import org.apache.druid.server.log.TestRequestLogger;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class DruidGrpcQueryRunnerTest
{
  private static final Logger logger = LoggerFactory.getLogger(DruidGrpcQueryRunnerTest.class);

  private static final QueryToolChestWarehouse warehouse = new MapQueryToolChestWarehouse(ImmutableMap.of());

  private static final ServiceEmitter noopServiceEmitter = new NoopServiceEmitter();

  private DruidGrpcQueryExecutor queryResource;
  private TestRequestLogger testRequestLogger;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void staticSetup()
  {
    EmittingLogger.registerEmitter(noopServiceEmitter);
  }

  @Before
  public void setup()
  {
    final ObjectMapper jsonMapper = new DefaultObjectMapper();
    jsonMapper.setInjectableValues(new InjectableValues.Std().addValue(ExprMacroTable.class, LookupEnabledTestExprMacroTable.INSTANCE)); // fails to unmarshal "doubleSum" otherwise

    testRequestLogger = new TestRequestLogger();
    queryResource = new DruidGrpcQueryExecutor(
      new QueryLifecycleFactory(
        warehouse,
        testDataset(),
        new DefaultGenericQueryMetricsFactory(),
        new NoopServiceEmitter(),
        testRequestLogger,
        new AuthConfig(),
        AuthTestUtils.TEST_AUTHORIZER_MAPPER
      ),
      jsonMapper);
  }

  private QuerySegmentWalker testDataset()
  {
    try {
      return CalciteTests.createMockWalker(
        CalciteTests.createQueryRunnerFactoryConglomerate().lhs,
        temporaryFolder.newFolder());
    }
    catch (Exception e) {
      throw new RuntimeException("Could not create test dataset", e);
    }
  }

  /**
   * schema: dim1,dim2,dim3 | m1, m2
   */
  private static final String groupByDimQuery =
      "{\n"
        + "    \"queryType\": \"groupBy\",\n"
        + "    \"dataSource\": \"foo\",\n"
        + "    \"dimensions\": [\"dim1\", \"dim2\"],\n"
        + "    \"granularity\": \"day\",\n"
        + "    \"intervals\": [\n"
        + "      \"2001-01-01T00:00:00.000/2001-01-05T00:00:00.000\"\n"
        + "    ],\n"
        + "    \"aggregations\": [\n"
        + "      {\n"
        + "        \"type\": \"doubleSum\",\n"
        + "        \"name\": \"M1\",\n"
        + "        \"fieldName\": \"m1\"\n"
        + "      },\n"
        + "      {\n"
        + "        \"type\": \"doubleSum\",\n"
        + "        \"name\": \"M2\",\n"
        + "        \"fieldName\": \"m2\"\n"
        + "      }\n"
        + "    ]\n"
        + "}";

  @Test
  public void testGoodQuery()
  {
    queryResource.execute(groupByDimQuery, new StreamObserver<GrpcRowBatch.RowBatchResponse>()
    {
      @Override
      public void onNext(GrpcRowBatch.RowBatchResponse response) {
        logger.info("onNext " + response);

        if (response.hasSchema()) {
          logger.info("   " + response.getSchema());
          //assertEquals(response.getSchema().getFieldsCount(), 4);
        }
        logger.info("   " + response.getBatch());
        logger.info("   " + response.getDictionary());

        if (!response.getBatch().isEmpty()) {
          final RowBatch batch = Marshallers.rowBatchMarshaller().unmarshal(response.getBatch().asReadOnlyByteBuffer());
          //assertEquals(batch.in, 2);
        }

        if (!response.getDictionary().isEmpty()) {
          final DictionaryEncoders.DictionaryEncoder dictionary = Marshallers.dictionaryMarshaller().unmarshal(response.getDictionary().toByteArray());
          assertEquals(4, dictionary.size());
        }
      }

      @Override
      public void onError(Throwable e) {
        logger.error("onError", e);
        //future.completeExceptionally(e);
      }

      @Override
      public void onCompleted() {
        logger.info("onCompleted");
        //future.complete(null);
      }
    });
  }


  @After
  public void tearDown()
  {
  }
}

