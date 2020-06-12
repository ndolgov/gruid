package net.ndolgov.druid.forecast

import java.util.concurrent.{ArrayBlockingQueue, BlockingQueue, TimeUnit}

import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchResponse
import org.apache.druid.server.grpc.common.{Marshallers, RowBatch}

trait RowBatchIterator extends Iterator[RowBatch] {
  def append(response: RowBatchResponse): Unit
}

/**
  * Single-consumer by design
  * @param timeoutMs
  */
class BlockingRowBatchIterator(timeoutMs: Long) extends RowBatchIterator {
  private val EOF = RowBatchResponse.getDefaultInstance

  private val responses: BlockingQueue[RowBatchResponse] = new ArrayBlockingQueue[RowBatchResponse](16)

  override def hasNext: Boolean = {
    val peeked = responses.peek
    (peeked == null) || (peeked != EOF) // more is coming or the next available is not EOF
  }

  override def next(): RowBatch =
    try {
      val response = responses.poll(timeoutMs, TimeUnit.MILLISECONDS)
      Marshallers.rowBatchMarshaller.unmarshal(response.getBatch.asReadOnlyByteBuffer)
    } catch {
      case _: InterruptedException =>
        throw new RuntimeException("Interrupted while waiting for next row batch")
    }

  override def append(response: RowBatchResponse): Unit =
    try {
      if (!response.getBatch.isEmpty) {
        responses.offer(response, timeoutMs, TimeUnit.MILLISECONDS)
      }

      val dictionaryBlob = response.getDictionary
      if (!dictionaryBlob.isEmpty) {
        val dictionary = Marshallers.dictionaryMarshaller.unmarshal(dictionaryBlob.toByteArray) // TODO how to return it?
        responses.offer(EOF, timeoutMs, TimeUnit.MILLISECONDS) // reached the end of the response stream
      }
    } catch {
      case _: InterruptedException =>
        throw new RuntimeException("Interrupted while waiting for next row batch")
    }

}
