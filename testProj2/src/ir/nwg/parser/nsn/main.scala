package ir.nwg.parser.nsn


import org.apache.spark.sql.catalyst.encoders
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import com.databricks.spark.xml._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{forall, input_file_name}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.JavaConversions.`deprecated seqAsJavaList`

object main extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("XML File Operations")
    .config("spark.testing.memory","471859200")
    .config("spark.sql.caseSensitive","false")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val df_config = spark.read.option("mode","FAILFAST").option("rowTag","config").xml("D:\\Work-Folder\\Config\\Configuration.xml")
  var columns: List[String] = List()
  df_config.foreach { row => {
      columns = row.getAs("columns").toString().trim().split(",").toList
    }
  }

  println("End reading parser config file.")
  println("*************************************************")
  println("*************************************************")
  println("Start reading and parsing raw xml files ... ")
  println("*************************************************")
  val df_xml_content = spark.read.format("com.databricks.spark.xml").
    option("rowTag", "Info").load("D:\\\\Work-Folder\\\\RawFiles\\\\*.xml").withColumn("file_name",input_file_name())

  //*************************************************
  //create dynamic schema based on config file
  //*************************************************
  var schema = StructType(Nil)
  for( item <- columns ) {
    schema = schema.add(StructField(item, IntegerType, true))
  }
  println("schema length is "+ schema.length)

  val colSeq = Seq(columns)

  val encoder = RowEncoder(schema)

  //*************************************************
  //parse each row of dataframe "df_xml_content"
  //*************************************************
  import spark.implicits._
  df_xml_content.show(truncate = false)
  val df_result = df_xml_content.map(row => {
      var Types = row.getAs("Types").toString().trim().split(" ") //contains column names from raw file
      var Values = row.getAs("Value").toString().trim().split(",") //contains: ObjName, Results
      var ObjName = Values(1) //object name
      var colValues = Values(0).subSequence(1, Values(0).toString().length).toString().trim().split(" ").map(x => x.toInt)
      //simple List are immutable and you get error in run-time if use it.
      var row_obj_list = scala.collection.mutable.ListBuffer.empty[Integer]

      //add column values:
      for (item <- columns) { //columns is from config file
        if (Types.contains(item.toString)) {
            row_obj_list += colValues(Types.indexOf(item))
        }
      }
      println("end of MAP Func for " + ObjName.toString)

      //to convert ListBuffer[Unit] to spark.sql.Row:
      var row_obj = Row.fromSeq(row_obj_list.toSeq)
      //You are not allowed to print or log anything after return value.
      //for return value of map function in scala, we are supposed to write object name
      //the retuen value type should be align with the encoder
      row_obj
    })(encoder)

  df_result.show()


}