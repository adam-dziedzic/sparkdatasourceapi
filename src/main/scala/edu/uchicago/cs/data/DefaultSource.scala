package edu.uchicago.cs.data

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider {

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
  
  def saveAsCsvFile(data: DataFrame, path: String) = {
    val dataCustomRDD = data.rdd.map(row => {
      val values = row.toSeq.map(value => value.toString)
      values.mkString(",")
    })
    dataCustomRDD.saveAsTextFile(path)
  }
  
  override def createRelation(sqlContext: SQLContext, mode: SaveMode, 
      parameters: Map[String, String], data: DataFrame): BaseRelation = {
    saveAsCsvFile(data, parameters.get("path").get)
    createRelation(sqlContext, parameters, data.schema)
  }
  
}