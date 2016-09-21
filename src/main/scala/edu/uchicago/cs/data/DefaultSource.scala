package edu.uchicago.cs.data

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType): BaseRelation = {
    parameters.getOrElse("path", sys.error("'path' must be specified for our data."))
    return new LegacyRelation(parameters.get("path").get, schema)(sqlContext)
  }
}