package spark.exampleSource.read.stream

import DefaultSource.getSchema
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{DataTypes, Metadata, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

class DefaultSource() extends TableProvider {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = getSchema

  override def getTable(schema: StructType, partitioning: Array[Transform], properties: util.Map[String, String]): Table = {
    new ExampleTable(schema, properties)
  }

  override def supportsExternalMetadata = true
}


object DefaultSource {
  def getSchema: StructType = {

    val structFields: Array[StructField] = Array[StructField] (
      StructField ("time", DataTypes.IntegerType, true, Metadata.empty),
      StructField ("data", DataTypes.IntegerType, true, Metadata.empty)
    )

    new StructType(structFields)
  }
}
