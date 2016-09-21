package edu.uchicago.cs.data

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD

class LegacyRelation(location: String, userSchema: StructType)(@transient val sqlContext: SQLContext)
    extends BaseRelation with TableScan with Serializable
    with Serializable {

  override def schema: StructType = {
    if (this.userSchema != null) {
      return this.userSchema
    } else {
      return StructType(
          Seq(
              StructField("name", StringType, true),
              StructField("age", IntegerType, true),
              StructField("city",StringType,true), 
              StructField("country",StringType,true)
           ))
    }
  }
  
  private def castValue(value: String, toType: DataType) = toType match {
    case _: StringType  => value
    case _: IntegerType => value.toInt
  }
  
  override def buildScan(): RDD[Row] = {
    /** The wholeTextFiles method read the entire files (each file is an entity), 
     *  reads the first two lines (the only fields we want) and creates a row from
     *  each of them.
     */
    val schemaFields = schema.fields
    val rdd = sqlContext.sparkContext.wholeTextFiles(location).map(x => x._2)
    
    val rows = rdd.map(file => { 
      val lines = file.split("\n")
      val typedValues = lines.zipWithIndex.map { 
        case (value, index) => {
          val dataType = schemaFields(index).dataType
          castValue(value, dataType)
        }
      }
      //Row.fromSeq(Seq(lines(0), lines(1)))
      Row.fromSeq(typedValues)
    })
    rows
  }

}