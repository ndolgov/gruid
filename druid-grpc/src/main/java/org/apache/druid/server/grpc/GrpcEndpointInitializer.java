package org.apache.druid.server.grpc;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.grpc.BindableService;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.grpc.server.GrpcServer;
import org.apache.druid.server.initialization.ServerConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;

/** Plug into Druid lifecycle management for eager initialization and graceful shut down */
@ManageLifecycle
public final class GrpcEndpointInitializer
{
  private final ServerConfig serverConfig;

  private final DruidNode druidNode;

  private final GrpcQueryExecutor queryExecutor;

  private GrpcServer server;

  @Inject
  public GrpcEndpointInitializer(ServerConfig serverConfig, @Self DruidNode druidNode, GrpcQueryExecutor queryExecutor)
  {
    this.serverConfig = serverConfig;
    this.druidNode = druidNode;
    this.queryExecutor = queryExecutor;
  }

  @LifecycleStart
  public void start()
  {
    server = create(
      executor -> new DruidRowBatchQueryService(executor, queryExecutor),
      serverConfig.getNumThreads(),
      druidNode.getHost(),
      druidNode.getPortToUse());

    server.start();
  }

  @LifecycleStop
  public void stop()
  {
    server.stop();
  }

  private GrpcServer create(Function<ExecutorService, BindableService> factory, int numThreads, String host, int port) {
    final int nHandlerThreads = numThreads; // TODO a reasonable way to size them up or add explicit gruid properties
    final int nServerThreads = numThreads;

    final GrpcServer srvr = new GrpcServer(
      host, port,
      newArrayList(factory.apply(executor(nHandlerThreads, "grpc-handler-%d"))),
      executor(nServerThreads, "grpc-server-%d"));

    return srvr;
  }

  private static ExecutorService executor(int nThreads, String name) {
    return Executors.newFixedThreadPool(nThreads, new ThreadFactoryBuilder().setNameFormat(name).build());
  }
}
