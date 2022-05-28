package spark.exampleSource.read.stream

import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class ExampleScanBuilder(val schema: StructType, val properties: util.Map[String, String], val options: CaseInsensitiveStringMap) extends ScanBuilder {
  override def build: Scan = new ExampleScan(schema, properties, options)
}

