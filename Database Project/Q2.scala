
/**
  * Created by prdin on 05-Jun-17.
  */

import java.io._
import java.lang._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.simba.index.RTreeType

object Q2 {
  case class POI(pid: Long, tags_poi: String, poi_lon: Double, poi_lat: Double)
  case class Car(cid:Long, sid: Long, car_lon: Double, car_lat: Double, days: String, times: String)

  def main(args: Array[String]): Unit = {

    val simba = SimbaSession.
      builder().
      master("local[*]").
      appName("Q2").
      config("simba.index.partitions", "8").
      getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    simba.sparkContext.setLogLevel("ERROR")
    System.setProperty("hadoop.home.dir","C:/winutils")


    //Schema for Trajectories dataset
    var schema = ScalaReflection.schemaFor[Car].dataType.asInstanceOf[StructType]
    val carsDS = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/trajectories.csv")
      .as[Car]

    carsDS.show(0)
    //Build a Rtree for Trajectories dataset
    carsDS.index(RTreeType, "carsRT", Array("car_lon", "car_lat"))
    carsDS.printSchema

    //Query finds all trajectories within a radius of 5000 metres with the given point as center
    val cdf=carsDS.circleRange(Array("car_lon","car_lat"),Array(-302967.241,4481467.241),5000)


    println(cdf.count())

    //Refine the results to obtain trajectories present only on Friday
    val fridays=cdf.filter(x => x.getString(4).contains("Friday"))
    fridays.show(10)

    //Write the results to file
    fridays.coalesce(1).
      write.
      format("com.databricks.spark.csv").option("header","true").save("Q2_fridays_Answer")

    //Refine the results to obtain only trajectories present only after 8PM
    val after8=fridays.filter(x => (x.getString(5).contains("20") || x.getString(5).contains("21") || x.getString(5).contains("22") || x.getString(5).contains("23")))

    println("Objects around Beijing International Airport on ")
    after8.show(10)
    //Write the results to file
   after8.coalesce(1).
      write.
      format("com.databricks.spark.csv").option("header","true").save("Q2_after8_Answer")
    simba.stop()

  }
}
