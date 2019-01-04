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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static com.google.common.collect.Lists.newArrayList;

/** Plug into Druid lifecycle management for eager initialization and graceful shut down */
@ManageLifecycle
public final class GrpcEndpointInitializer
{
  private final GrpcConfig grpcConfig;

  private final DruidNode druidNode;

  private final GrpcQueryExecutor queryExecutor;

  private GrpcServer server;

  @Inject
  public GrpcEndpointInitializer(GrpcConfig grpcConfig, @Self DruidNode druidNode, GrpcQueryExecutor queryExecutor)
  {
    this.grpcConfig = grpcConfig;
    this.druidNode = druidNode;
    this.queryExecutor = queryExecutor;
  }

  @LifecycleStart
  public void start()
  {
    final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(GrpcEndpointInitializer.class.getClassLoader());

      server = create(
        executor -> new DruidRowBatchQueryService(executor, queryExecutor),
        grpcConfig.getNumHandlerThreads(),
        grpcConfig.getNumServerThreads(),
        druidNode.getHost(),
        grpcConfig.getPort());

      server.start();
    } finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }

  @LifecycleStop
  public void stop()
  {
    server.stop();
  }

  private GrpcServer create(Function<ExecutorService, BindableService> factory, int nHandlerThreads, int nServerThreads, String host, int port) {
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
