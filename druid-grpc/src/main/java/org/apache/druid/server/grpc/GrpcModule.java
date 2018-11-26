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

import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.grpc.BindableService;
import org.apache.druid.guice.Jerseys;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.grpc.server.GrpcServer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;

public final class GrpcModule implements DruidModule
{
  private static final String PROPERTY_RPC_ENABLE = "druid.rpc.enable";

  @Inject
  private Properties props;

  @Override
  public void configure(Binder binder)
  {
    if (isEnabled()) {
      Jerseys.addResource(binder, DruidGrpcQueryExecutor.class);
      binder.bind(GrpcQueryExecutor.class).to(DruidGrpcQueryExecutor.class);
    }
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_RPC_ENABLE, "false"));
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  private static class GrpcInitializer {
    private GrpcServer server;

    public GrpcServer startUp(String host, int port, Function<Executor, BindableService> factory, int nServerThreads, int nHandlerThreads) {
      server = new GrpcServer(
        host,
        port,
        newArrayList(factory.apply(executor(nHandlerThreads, "grpc-handler-%d"))),
        executor(nServerThreads, "grpc-server-%d"));

      server.start();
      return server;
    }

    public void shutDown() {
      server.stop();
    }

    private static ExecutorService executor(int nThreads, String name) {
      return Executors.newFixedThreadPool(nThreads, new ThreadFactoryBuilder().setNameFormat(name).build());
    }
  }
}
