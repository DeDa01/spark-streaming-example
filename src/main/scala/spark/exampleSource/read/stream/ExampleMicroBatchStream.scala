package spark.exampleSource.read.stream

import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.LongOffset
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util



class ExampleMicroBatchStream(val schema: StructType, val properties: util.Map[String, String], val options: CaseInsensitiveStringMap) extends MicroBatchStream {

  private val minOffset = options.getLong("minOffset", 0)

  override def latestOffset: LongOffset = LongOffset(System.currentTimeMillis() / 1000)

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    println("planInputPartitions: " + start + " " + end)
    val s = start.asInstanceOf[LongOffset].offset.toInt + 1
    val e = end.asInstanceOf[LongOffset].offset.toInt

    Range.inclusive(s, e).map(b => new ExampleInputPartition(b)).toArray
  }

  override def createReaderFactory: PartitionReaderFactory = new ExamplePartitionReaderFactory(schema)

  override def initialOffset: Offset = {
    println("initialOffset")
    new LongOffset(minOffset)
  }

  override def deserializeOffset(json: String): Offset = {
    println("deserializeOffset: " + json)
    new LongOffset(json.toLong)
  }

  override def commit(end: Offset): Unit = {

  }

  override def stop(): Unit = {

  }

}
