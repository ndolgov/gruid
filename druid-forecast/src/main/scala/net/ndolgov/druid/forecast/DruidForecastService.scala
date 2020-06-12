package net.ndolgov.druid.forecast

import io.grpc.stub.StreamObserver
import net.ndolgov.druid.forecast.aggregation.Op
import org.apache.druid.server.grpc.GrpcRowBatch.{QueryRequest, RowBatchResponse}
import org.apache.druid.server.grpc.client.GrpcClient
import org.apache.druid.server.grpc.common.RowBatchReaders.RowBuffer
import org.apache.druid.server.grpc.common.ToQuerySchema
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

class DruidForecastService(val service: ForecastService,
                           val client: GrpcClient,
                           val ec: ExecutionContext,
                           val bufferTimeout: Long) {
  private val logger = LoggerFactory.getLogger(classOf[DruidForecastService])

  def execute(request: ForecastRequest): Future[_] = {
    val batches = new BlockingRowBatchIterator(bufferTimeout)
    val (opFuture, completionFuture) = executeDruidQuery(request, batches)(ec)

    val executionFuture = opFuture.map(op => {
      logger.info("Running ops chain")
      op.exec(null)
      logger.info("Ops chain finished")
    })(ec)

    executionFuture.flatMap(_ => completionFuture)(ec)
  }

  private def executeDruidQuery(request: ForecastRequest, responses: RowBatchIterator)
                               (implicit ec: ExecutionContext): (Future[Op[RowBuffer]], Future[_]) = {
    val opPromise = Promise[Op[RowBuffer]]() // to return the root operator
    val completionPromise = Promise[Unit]() // to signal reaching the end of inout stream

    val druidQueryRequest = QueryRequest.newBuilder.setQuery(request.druidQuery).build()

    client.call(druidQueryRequest, new StreamObserver[RowBatchResponse]() {
      override def onNext(response: RowBatchResponse): Unit = {
        try {
          if (response.hasSchema) {
            opPromise.complete(Try(
                service.forecast(
                  ToQuerySchema.convert(response.getSchema),
                  responses,
                  request)))
          }

          responses.append(response)
        } catch {
          case e: Exception =>
            logger.error("Could not process row batch ", e)
            completionPromise.failure(e)
        }
      }

      override def onError(e: Throwable): Unit = {
        logger.error("Druid query failed", e)
        completionPromise.failure(e)
      }

      override def onCompleted(): Unit = {
        logger.info("Druid query finished")
        completionPromise.complete(Try(()))
      }
    })

    (opPromise.future, completionPromise.future)
  }
}
