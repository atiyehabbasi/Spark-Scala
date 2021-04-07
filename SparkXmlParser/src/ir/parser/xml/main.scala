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

  println("*************************************************")
  println("Start reading and parsing raw xml files ... ")
  println("*************************************************")
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
      var Values = row.getAs("Value").toString().trim().split(",") //contains: Name, Results
      var Name = Values(1) //Name
      var colValues = Values(0).subSequence(1, Values(0).toString().length).toString().trim().split(" ").map(x => x.toInt)
      //simple List are immutable and you get error in run-time if use it.
      var row_obj_list = scala.collection.mutable.ListBuffer.empty[Integer]

      //add column values:
      for (item <- selectedColumns) {
        if (Types.contains(item.toString)) {
            row_obj_list += colValues(Types.indexOf(item))
        }
      }
      println("end of MAP Func for " + Name.toString)

      //to convert ListBuffer[Unit] to spark.sql.Row:
      var row_obj = Row.fromSeq(row_obj_list.toSeq)
      //You are not allowed to print or log anything after return value.
      //for return value of map function in scala, we are supposed to write object name
      //the retuen value type should be align with the encoder
      row_obj
    })(encoder)

  df_result.show()


}
/* This is a sample raw file:
<?xml version="1.0" encoding="UTF-8"?>
<Data>
    <Info InfoId="Category1">
      <Types>COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8026 COL8027 COL8028 COL8029 COL8030 COL8031 COL8032</Types>
      <Value Name="GenX">
        <Results>100 101 102 103 0 0 106 0 0 0 0 0 112</Results>
      </Value>
    </Info>
	<Info InfoId="Category2">
      <Types>COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8032</Types>
      <Value Name="GenY">
        <Results>200 201 202 203 204 205 212</Results>
      </Value>
    </Info>
</Data>
 */

/* Read Result:
+-------------------------------------------------------------------------------------------------------+---------------------------------------------+---------+----------------------------------------------------------------------------+
|Types                                                                                                  |Value                                        |_InfoId  |file_name                                                                   |
+-------------------------------------------------------------------------------------------------------+---------------------------------------------+---------+----------------------------------------------------------------------------+
|COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8026 COL8027 COL8028 COL8029 COL8030 COL8031 COL8032|[100 101 102 103 0 0 106 0 0 0 0 0 112, GenX]|Category1|file:/D:/Work-Folder/RawFiles/RawFile_BeginDate20210704_aslkjh4878623343.xml|
|COL8020 COL8021 COL8022 COL8023 COL8024 COL8025 COL8032                                                |[200 201 202 203 204 205 212, GenY]          |Category2|file:/D:/Work-Folder/RawFiles/RawFile_BeginDate20210704_aslkjh4878623343.xml|
+-------------------------------------------------------------------------------------------------------+---------------------------------------------+---------+----------------------------------------------------------------------------+

Parse Result:
end of MAP Func for GenX]
end of MAP Func for GenY]
+-------+-------+-------+-------+
|COL8020|COL8022|COL8023|COL8032|
+-------+-------+-------+-------+
|    100|    102|    103|    112|
|    200|    202|    203|    212|
+-------+-------+-------+-------+

 */