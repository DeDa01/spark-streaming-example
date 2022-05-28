package spark.exampleSource.read.stream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

class ExamplePartitionReader(val inputPartition: ExampleInputPartition, val schema: StructType)
  extends PartitionReader[InternalRow] {

  var counter = 0

  override def next: Boolean = {
    counter += 1
    counter < 10
  }

  override def get: InternalRow = {
    val values = List(
      inputPartition.time,
      (inputPartition.time + counter) % 1000
    )

    InternalRow.fromSeq(values)
  }

  override def close(): Unit = {

  }
}

