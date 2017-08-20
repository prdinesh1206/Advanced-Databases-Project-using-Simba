
/**
  * Created by prdin on 05-Jun-17.
  */

import java.io._
import java.lang._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.simba.index.RTreeType

object Q4{
  case class POI(pid: Long, tags_poi: String, poi_lon: Double, poi_lat: Double)
  case class Car(cid:Long, sid: Long, car_lon: Double, car_lat: Double, days: String, times: String)

  def main(args: Array[String]): Unit = {

    val start_time=System.currentTimeMillis()
    val simba = SimbaSession.
      builder().
      master("local[*]"). //Change * to 1 for executing the program using 1 core
      appName("Q4").
      config("simba.index.partitions", "8").
      getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    simba.sparkContext.setLogLevel("ERROR")
    System.setProperty("hadoop.home.dir","C:/winutils")


    //Schema for POIs dataset
    var schema = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]
    val poisDS = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/POIs.csv")
      .as[POI]


    poisDS.show(10)
    //Build RTree for POIs dataset
   val poi_index=poisDS.index(RTreeType, "poisRT", Array("poi_lon", "poi_lat"))
    poisDS.printSchema

    poisDS.show(10)

    //Schema for Trajectories dataset
    schema = ScalaReflection.schemaFor[Car].dataType.asInstanceOf[StructType]
    val carsDS = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/trajectories.csv")
      .as[Car]

    carsDS.show(0)

    //Build RTree for Trajectories dataset
   val traj_index= carsDS.index(RTreeType, "carsRT", Array("car_lon", "car_lat"))
    carsDS.printSchema

    //Do distance join to obtain objects around POIs

    val dj = poi_index.distanceJoin(traj_index,
      Array("poi_lon", "poi_lat"),
      Array("car_lon", "car_lat"),
      100.0)
    dj.show(20)

    //Refine results to obtain objects present only during weekends
    val weekends=dj.filter(x => (x.getString(8).contains("Saturday") || x.getString(8).contains("Sunday")))
    //Refine results to obtain objects present only in the year 2009
    val years=weekends.filter(x => x.getString(8).contains("2009"))

    val groupedyears=years.groupBy("poi_lon","poi_lat").count()
     val sortedyears=groupedyears.sort($"count".desc)
    println("Popular POIs in 2009 weekends")
    sortedyears.show(10)

    //Print the results

    println("Time Taken exculding file write is : ")
    println(System.currentTimeMillis()-start_time)
    //Write the results to the file years.csv

  /*  sortedyears.coalesce(1).
      write.
      format("com.databricks.spark.csv").option("header","true").save("Q4_final_answer_1.csv")
   // years.show(20)
*/
    //println(years.count())

    simba.stop()

  }
}
