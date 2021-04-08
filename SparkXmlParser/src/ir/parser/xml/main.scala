package ir.parser.xml

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

  //read the file using databricks
  val df_xml_content = spark.read.format("com.databricks.spark.xml").
    option("rowTag", "Info").load("D:\\\\Work-Folder\\\\RawFiles\\\\*.xml").withColumn("file_name",input_file_name())

  //*************************************************
  //create dynamic schema
  //*************************************************
  var selectedColumns: List[String] = List("COL8020","COL8022","COL8023","COL8032")
  var schema = StructType(Nil)
  for( item <- selectedColumns ) {
    schema = schema.add(StructField(item, IntegerType, true))
  }
  val colSeq = Seq(selectedColumns)
  val encoder = RowEncoder(schema)

  //*************************************************
  //parse each row of dataframe "df_xml_content"
  //*************************************************
  import spark.implicits._
  df_xml_content.show(truncate = false)

  val df_result = df_xml_content.map(row => {
      var Types = row.getAs("Types").toString().trim().split(" ") //contains column names from raw file
      var Values = row.getAs("Value").toString().trim().split(" ").map(x => x.toInt) //contains column values
      //simple List are immutable and you will get error in run-time if use it.
      var row_obj_list = scala.collection.mutable.ListBuffer.empty[Integer]

      //add column values:
      for (item <- selectedColumns) {
        if (Types.contains(item.toString)) {
            row_obj_list += Values(Types.indexOf(item))
        }
      }
      println("end of MAP Func for " + row.getAs("_InfoId").toString().trim())

      //to convert ListBuffer[Unit] to spark.sql.Row:
      var row_obj = Row.fromSeq(row_obj_list.toSeq)
      //You are not allowed to print or log anything after return value. since there is not return keword here, you may forget it :)
      //for return value of map function in scala, we are supposed to write object name
      //the retuen value type should be align with the encoder
      row_obj
    })(encoder)

  df_result.show()


}
/* This is a sample raw file:
<?xml version="1.0" encoding="UTF-8"?>
<Data>
    <Info InfoId="GenX">
      <Types>COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8026 COL8027 COL8028 COL8029 COL8030 COL8031 COL8032</Types>
      <Value>100 101 102 103 0 0 106 0 0 0 0 0 112</Value>
    </Info>
	<Info InfoId="GenY">
      <Types>COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8032</Types>
      <Value>200 201 202 203 204 205 212</Value>
    </Info>
</Data>
 */

/* Read Result:
+-------------------------------------------------------------------------------------------------------+-------------------------------------+-------+----------------------------------------------------------------------------+
|Types                                                                                                  |Value                                |_InfoId|file_name                                                                   |
+-------------------------------------------------------------------------------------------------------+-------------------------------------+-------+----------------------------------------------------------------------------+
|COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8026 COL8027 COL8028 COL8029 COL8030 COL8031 COL8032|100 101 102 103 0 0 106 0 0 0 0 0 112|GenX   |file:/D:/Work-Folder/RawFiles/RawFile_BeginDate20210704_aslkjh4878623343.xml|
|COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8032                                                |200 201 202 203 204 205 212          |GenY   |file:/D:/Work-Folder/RawFiles/RawFile_BeginDate20210704_aslkjh4878623343.xml|
+-------------------------------------------------------------------------------------------------------+-------------------------------------+-------+----------------------------------------------------------------------------+

Parse Result:
end of MAP Func for GenX
end of MAP Func for GenY
+-------+-------+-------+-------+
|COL8020|COL8022|COL8023|COL8032|
+-------+-------+-------+-------+
|    100|    102|    103|    112|
|    200|    202|    203|    212|
+-------+-------+-------+-------+

 */