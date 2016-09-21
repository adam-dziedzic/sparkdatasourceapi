package edu.uchicago.cs.data

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object app {  
  
  def main(args: Array[String]) {
    val appName = "testing provider"
    val spark = SparkSession.builder().appName(appName).getOrCreate()
   
    val df = spark
              .read
              .format("edu.uchicago.cs.data")
              .load("/home/adam/data/sparkDataSourceAPI")   
              
    df.printSchema()
    df.show()
    
    df.select("name").show()
    
    import spark.implicits._
    println("Filter users who are more than 21 years old:")
    df.filter($"age" > 21).show()
    
    df.createOrReplaceTempView("users")
    val sqlDF = spark.sql("select * from users")
    sqlDF.show()
    
    val names = spark.sql("select name from users")
    names.show()
    
    val ages = spark.sql("select age from users")
    ages.show()
    
  }
}
