package spark.exampleSource.read.stream

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

class ExamplePartitionReaderFactory(val schema: StructType) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition) = new ExamplePartitionReader(partition.asInstanceOf[ExampleInputPartition], schema)
}