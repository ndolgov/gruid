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
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LifecycleModule;
import org.apache.druid.initialization.DruidModule;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public final class GrpcModule implements DruidModule
{
  private static final String PROPERTY_GRPC_ENABLE = "druid.grpc.enable";

  @Inject
  private Properties props;

  @Override
  public void configure(Binder binder)
  {
    if (isEnabled()) {
      JsonConfigProvider.bind(binder, "druid.grpc", GrpcConfig.class);

      binder.bind(GrpcQueryExecutor.class).to(DruidGrpcQueryExecutor.class).in(Singleton.class);

      LifecycleModule.register(binder, GrpcEndpointInitializer.class);
    }
  }

  private boolean isEnabled()
  {
    Preconditions.checkNotNull(props, "props");
    return Boolean.valueOf(props.getProperty(PROPERTY_GRPC_ENABLE, "false"));
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

}
