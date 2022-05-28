package spark.exampleSource.read.stream

import org.apache.spark.sql.connector.read.InputPartition

class ExampleInputPartition(var time: Int) extends InputPartition {
  override def preferredLocations = new Array[String](0) // No preferred location
}

