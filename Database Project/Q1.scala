
/**
  * Created by prdin on 05-Jun-17.
  */

import java.io._
import java.lang._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.simba.index.RTreeType

object Q1 {
  case class POI(pid: Long, tags_poi: String, poi_lon: java.lang.Double, poi_lat: java.lang.Double)
  case class Car(cid:Long, sid: Long, car_lon: Double, car_lat: Double, days: String, times: String)

  def main(args: Array[String]): Unit = {

    val simba = SimbaSession.
      builder().
      master("local[*]").
      appName("Q1").
      config("simba.index.partitions", "8").
      getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    simba.sparkContext.setLogLevel("ERROR")
    System.setProperty("hadoop.home.dir","C:/winutils")


    var schema = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]
    val poisDS = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/POIs.csv")
      .as[POI]


    poisDS.show(10)
    //poisDS.filter(x => x.poi_lon== null).show(10)
    poisDS.index(RTreeType, "poisRT", Array("poi_lon", "poi_lat"))
    poisDS.printSchema

    //Given points of MBR
    val pointA=Array(-332729.310,4456050.000)
    val pointB=Array(-316725.862,4469518.966);

    poisDS.show(10)

    //Do a range query to obtain all the POIs inside the coordinates
    val ddf=poisDS.range(Array("poi_lon","poi_lat"),Array(-332729.310,4456050.000),Array(-316725.862,4469518.966))

    poisDS.show(10)
    val shops=ddf.filter(x => x.getString(1).contains("shop")) //Retrieve all the rows with tags containing shops

    //Writing output to the file

    shops.coalesce(1).
    write.
    format("com.databricks.spark.csv").option("header","true").save("Q1_Answer")
    schema = ScalaReflection.schemaFor[Car].dataType.asInstanceOf[StructType]

    simba.stop()

  }
}
