package com.ingest.data.filetemplate

import com.ingest.data.common._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}



object FileTemplate extends LazyLogging with SparkFixture {

  def main(args: Array[String]): Unit = {

    if(args.length == 10){
      val targetTable = args(0).trim
      val inputFilePath = args(1).trim
      val schemaString = args(2).toLowerCase.trim
      val inputFileFormat = args(3).trim
      val outputFileFormat = args(4).trim
      val writeMode = args(5).trim
      val readOptionsRaw = args(6).trim
      val customQuery = args(7).trim
      val partitionCols = args(8).trim
      val outputPath = args(9).trim

      val inputReadOptions: Map[String, String] = generateReadOptions(readOptionsRaw)

      logger.info(s" $partitionCols and $customQuery ")
      val inputFileDf = readInputFile(inputFilePath,inputFileFormat,schemaString,inputReadOptions.filter(_._2.nonEmpty))
      if(customQuery.nonEmpty){
        val filteredDf = filterInputData(inputFileDf, targetTable, customQuery)
        logger.info("filterValue rdd is :"+filteredDf.rdd.getNumPartitions)
        writeToFile(filteredDf, outputPath, writeMode, outputFileFormat, partitionCols)
      }
      else{
        logger.info("full table read")
        writeToFile(inputFileDf, outputPath, writeMode, outputFileFormat, partitionCols)
      }
    }
    else{
      logger.info("Expected number of arguments is 10 and provided is "+args.length)
      System.exit(1)
    }

  }

  def getDataTypes(dataType: String): DataType = {
    val returnType= dataType match {
      case "string" => StringType
      case "int" | "integer" => IntegerType
      case "long" => LongType
      case "timestamp" => TimestampType
      case "date" => DateType
      case "float" => FloatType
      case "double" => DoubleType
      case "boolean" => BooleanType
      case _ => StringType
    }
    returnType
  }

  def generateSchema(schemaString: String): StructType = {
    val fields = schemaString.split("-").map(fieldName =>
      StructField(fieldName.split(":")(0), getDataTypes(fieldName.split(":")(1)), nullable = true))
    StructType(fields)
  }

  def readInputFile(filePath:String,readFormat:String,schemaStr:String,readOptions:Map[String,String]): DataFrame = {
    if(schemaStr.nonEmpty & readOptions.nonEmpty)
      spark.read
        .format(readFormat)
        .options(readOptions)
        .schema(generateSchema(schemaStr))
        .load(filePath)
    else if(schemaStr.nonEmpty & readOptions.isEmpty)
      spark.read
        .format(readFormat)
        .schema(generateSchema(schemaStr))
        .load(filePath)
    else if (schemaStr.isEmpty & readOptions.nonEmpty)
      spark.read
        .format(readFormat)
        .options(readOptions)
        .load(filePath)
    else
      spark.read
        .format(readFormat)
        .load(filePath)

  }

  def writeToFile(df: DataFrame,outputPath: String,writeMode: String,outputFileFormat: String,partitionValue: String)
                 (implicit spark: SparkSession): Unit = {
    if(partitionValue.trim.equalsIgnoreCase("null")){
      df.write
        .mode(writeMode)
        .format(outputFileFormat)
        .save(outputPath)
    }
    else {
      df.write
        .mode(writeMode)
        .format(outputFileFormat)
        .partitionBy(partitionValue.split(",").mkString(","))
        .save(outputPath)
    }
  }

  def filterInputData(inputDf: DataFrame, targetTable:String, customQuery: String)
                     (implicit spark: SparkSession): DataFrame = {
    inputDf.createOrReplaceTempView(targetTable)
    spark.sql(customQuery.replaceAll("#",",").replaceAll("\\^","\\\""))
  }

  def generateReadOptions(readOptionsRaw: String): Map[String,String] = {
    var readOptions : Map[String,String] = Map()
    val readOption = readOptionsRaw.split("-")
    if(readOptionsRaw.nonEmpty) {
      readOption.map { x =>
        val keyValue = x.split("#")
        if (keyValue(1).trim.nonEmpty) {
          val sep = if (keyValue(0).equalsIgnoreCase("sep") & keyValue(1).equalsIgnoreCase("null")) "\u0000"
                    else keyValue(1)
          if (keyValue(0).equalsIgnoreCase("sep")) readOptions += (keyValue(0) -> sep)
          else readOptions += (keyValue(0) -> keyValue(1))
        }
      }
    }
    readOptions
  }



}
