package com.ingest.data.filetemplate

import java.io.File
import java.nio.file.{Files, Path}
import com.ingest.data.common.SparkFixture
import org.specs2.mutable.Specification
import com.ingest.data.filetemplate.FileTemplate._

class FilesTemplateSpec extends Specification with SparkFixture {
  //val metadataFilePath = (getClass.getResource("/").getPath).split("pesync/")(0)+"jobs/pe-sync/scripts/driver_file.csv"
  val metadataFilePath = getClass.getResource("/metadata/").getPath+"driver_file.csv"
  val tmpDir: Path = Files.createTempDirectory(getClass.getSimpleName)
  val outputPath: String = new File(tmpDir.toFile, "output").getAbsolutePath
  println("Driver File : "+metadataFilePath)

  val lines: Array[String] = scala.io.Source.fromFile(metadataFilePath).getLines().toArray

  val target_table = 0
  val input_path = 1
  val input_schema = 2
  val input_format = 3
  val output_format = 4
  val write_mode = 5
  val output_path = 6
  val read_options = 7
  val custom_query = 8
  val partition_col = 9
  val frequency = 10
  val size = 11

  "Metadata column " should {
    var flag=true
    //val df=spark.read.parquet(confParams.inputFile)
    val i = 1
    " match the expected # of columns" in {
      for(row <- lines){
          if(row.split(",").length!=12){
            flag=false
        }
      }
      flag.mustEqual(true)
    }
  }

  "Create read options " should {
    "parse valid options " in {
      val possibleOptions = "sep#null-nullValue#\\N-header#true-test# "
      val opt= generateReadOptions(possibleOptions)
      opt.get("header") mustEqual(Some("true"))
      opt.get("sep") mustEqual(Some("\u0000"))
      opt.get("nullValue") mustEqual(Some("\\N"))
      opt.get("test") mustEqual(None)
    }

  }

  "Framework " should {
    val i = 1
    "Parse valid textfile format data"  in {
      val metadata = lines.filter(_.split(",")(target_table).equals("web_track" )).mkString(",").split(",")
      println(metadata(target_table))
      val df = readInputFile(getClass.getResource("/textfile/").getPath,metadata(input_format),metadata(input_schema)
        ,generateReadOptions(metadata(read_options)))
      df.count() must_==(22)
    }

    "Parse valid csv format data"  in {
      val metadata = lines.filter(_.split(",")(target_table).equals("test_csv" )).mkString(",").split(",")
      println(metadata(target_table))
      val df = readInputFile(getClass.getResource("/csv/").getPath,metadata(input_format),metadata(input_schema)
        ,generateReadOptions(metadata(read_options)))
      val filteredDf = filterInputData(df, metadata(target_table),metadata(custom_query))
      filteredDf.show()
      //writeToFile(df,outputPath,metadata(write_mode),metadata(output_format),metadata(partition_col))
      df.count() must_==(20)

    }

  }


}
