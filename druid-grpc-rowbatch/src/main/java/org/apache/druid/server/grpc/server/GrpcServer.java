package org.apache.druid.server.grpc.server;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.ExecutorService;

/**
 * Create a GRPC server for given request handlers and bind it to the provided TCP address.
 */
public final class GrpcServer {
    private static final Logger logger = LoggerFactory.getLogger(GrpcServer.class);

    private final InetSocketAddress address;

    private final Server server;

    public GrpcServer(String hostname, int port, Collection<BindableService> services, ExecutorService executor) {
        address = new InetSocketAddress(hostname, port);

        final ServerBuilder builder = NettyServerBuilder.forAddress(address);
        services.forEach(builder::addService);
        server = builder.executor(executor).build();
    }

    public void start() {
        try {
            logger.info("Starting " + this);
            server.start();
            logger.info("Started " + this);
        } catch (Exception e) {
            throw new RuntimeException("Could not start server", e);
        }
    }

    public void stop() {
        try {
            logger.info("Stopping " + this);
            server.shutdown();
            logger.info("Stopped " + this);
        } catch (Exception e) {
            logger.warn("Interrupted while shutting down " + this);
        }
    }

    @Override
    public String toString() {
        return "{GrpcServer:addr=" + address + "}";
    }
}
