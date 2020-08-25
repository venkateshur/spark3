package examples

import org.apache.spark.sql.functions.col


object Example1 extends App with SparkSessionProvider {

  import spark.implicits._

  private final val props = Array("delta.optimizer", "delta.compat")

  val sampleDf = Seq(
    ("table_properties", "delta.optimizer=true, delta.compat=false")
  ).toDF("properties", "values")

  val newDf = sampleDf.filter(col("properties") === "table_properties")
  val propsMap1 = newDf.select("values").collect()
  val propsMap = newDf.select("values").collect().flatMap(_.toString
    .replaceAll("\\[", "").replaceAll("]", "").split(",").map{prop =>
    val split = prop.split("=")
    (split(0), split(1))
  }).toMap

  val propsToBeUpdated = props.filter(propToBeChecked => propsMap.getOrElse(propToBeChecked, "false").toBoolean)
  val updated = propsToBeUpdated.map(prop => s"$prop=true").mkString(", ")

  //take this updated string and put in table properties command

}
