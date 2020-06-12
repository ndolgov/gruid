package net.ndolgov.druid.forecast

import java.util

import io.grpc.BindableService
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField
import org.apache.druid.server.grpc.GrpcRowBatch.RowBatchSchema.RowBatchField.RowBatchFieldType
import org.apache.druid.server.grpc.common.{DictionaryEncoders, RowBatch, TestRowBatchQueryServices}
import org.apache.druid.server.grpc.server.GrpcServer
import org.joda.time.DateTime

object TestDruidServer {
  val Capacity = 5

  def server(hostname: String, port: Int, services: util.List[BindableService] = bindableService()): GrpcServer = {
    new GrpcServer(
      hostname,
      port,
      services,
      TestRowBatchQueryServices.executor("rpc-server-%d"))
  }

  def bindableService(): util.List[BindableService] = {
    val dictionary = DictionaryEncoders.dictionaryEncoder
    dictionary.encode("USA")
    dictionary.encode("CAN")

    dictionary.encode("iOS")
    dictionary.encode("Android")

    TestRowBatchQueryServices.bindableService(schema(), dictionary, testRowBatch())
  }

  def schema(): RowBatchSchema = {
    val schema = RowBatchSchema.newBuilder
      .addFields(RowBatchField.newBuilder.setFieldName("Time").setFieldType(RowBatchFieldType.TIME))
      .addFields(RowBatchField.newBuilder.setFieldName("Country").setFieldType(RowBatchFieldType.DIMENSION))
      .addFields(RowBatchField.newBuilder.setFieldName("Product").setFieldType(RowBatchFieldType.DIMENSION))
      .addFields(RowBatchField.newBuilder.setFieldName("Count").setFieldType(RowBatchFieldType.DOUBLE_METRIC)) // todo LONG_METRIC
      .addFields(RowBatchField.newBuilder.setFieldName("Weight").setFieldType(RowBatchFieldType.DOUBLE_METRIC))
      .build
    schema
  }

  def testRowBatch(): RowBatch = {
    val longColumns = Array.ofDim[Long](1, Capacity)
    val doubleColumns = Array.ofDim[Double](2, Capacity)
    val intColumns = Array.ofDim[Int](2, Capacity)

    // Time
    longColumns(0)(0) = DateTime.parse("2020-05-25T00:00:00.000Z").getMillis
    longColumns(0)(1) = DateTime.parse("2020-05-26T00:00:00.000Z").getMillis
    longColumns(0)(2) = DateTime.parse("2020-05-27T00:00:00.000Z").getMillis
    longColumns(0)(3) = DateTime.parse("2020-05-28T00:00:00.000Z").getMillis
    longColumns(0)(4) = DateTime.parse("2020-05-29T00:00:00.000Z").getMillis

    // Country
    intColumns(0)(0) = 0
    intColumns(0)(1) = 0
    intColumns(0)(2) = 1
    intColumns(0)(3) = 1
    intColumns(0)(4) = 1

    // Product
    intColumns(1)(0) = 2
    intColumns(1)(1) = 2
    intColumns(1)(2) = 2
    intColumns(1)(3) = 2
    intColumns(1)(4) = 3

    // Count
    doubleColumns(0)(0) = 5
    doubleColumns(0)(1) = 10
    doubleColumns(0)(2) = 15
    doubleColumns(0)(3) = 20
    doubleColumns(0)(4) = 25

    // Weight
    doubleColumns(1)(0) = 3.14
    doubleColumns(1)(1) = 2.71
    doubleColumns(1)(2) = 3.14
    doubleColumns(1)(3) = 2.71
    doubleColumns(1)(4) = 3.14

    new RowBatch(longColumns, intColumns, doubleColumns, Capacity, Capacity)
  }
}
