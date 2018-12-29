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

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Custom "druid.grpc.xxx" properties to be set in Druid broker configuration file (e.g. broker/runtime.properties)
 */
public class GrpcConfig
{
  @JsonProperty
  @Max(0xffff)
  private int port = -1;

  @JsonProperty
  @Min(1)
  private int numHandlerThreads = Runtime.getRuntime().availableProcessors();

  @JsonProperty
  @Min(1)
  private int numServerThreads = Runtime.getRuntime().availableProcessors();

  /**
   * @return the size of the thread pool on which queries are actually executed
   */
  public int getNumHandlerThreads()
  {
    return numHandlerThreads;
  }

  /**
   * @return the size of the thread pool used by gRPC transport for IO
   */
  public int getNumServerThreads()
  {
    return numServerThreads;
  }

  /**
   * @return the port to accept gRPC client connections on
   */
  public int getPort()
  {
    return port;
  }
}
