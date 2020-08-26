package examples

import org.apache.spark.sql.functions.col


object Example1 extends App with SparkSessionProvider {

  import spark.implicits._

  private final val props = Map("delta.optimizer" -> "true", "delta.compat" -> "true", "serilaization" -> "write", "delta.compat1" -> "false")

  val sampleDf = Seq(
    ("table_properties", "delta.optimizer=true, delta.compat=false, delta.compat1=false")
  ).toDF("properties", "values")

  val newDf = sampleDf.filter(col("properties") === "table_properties")
  val propsMap1 = newDf.select("values").collect()
  val propsMap = newDf.select("values").collect().flatMap(_.toString
    .replaceAll("\\[", "").replaceAll("]", "").split(",").map{prop =>
    val split = prop.split("=")
    (split(0).trim, split(1).trim)
  }).toMap

  val propsToBeUpdated = props.map{case (key, value) =>  {
    propsMap.contains(key) match {
      case true if !propsMap(key).equalsIgnoreCase(value.trim) => Map(key -> value)
      case false =>  Map(key -> value)
      case _ =>   Map.empty[String, String]
    }
  }}.reduce(_ ++ _).map{case(key,value) => s"$key=$value"}.mkString(", ")

  propsToBeUpdated

  //take this updated string and put in table properties command

}
