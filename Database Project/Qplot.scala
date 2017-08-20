
/**
  * Created by prdin on 05-Jun-17.
  */

//import java.util._
import java.lang._
import scala.collection.JavaConverters._


import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.Row

object Qplot{
  case class POI(pid: Long, tags_poi: String, poi_lon: Double, poi_lat: Double)
  case class Car(cid:Long, sid: Long, car_lon: Double, car_lat: Double, days: String, times: String)
  case class POIArray(min_lat: Double, min_long: Double, max_lat: Double, max_long: Double)
  def main(args: Array[String]): Unit = {

    val start_time=System.currentTimeMillis()
    val simba = SimbaSession.
      builder().
      master("local[1]"). //Change * to 1 for executing the program using 1 core
      appName("Qplot").
      config("simba.index.partitions", "16").
      getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    simba.sparkContext.setLogLevel("ERROR")
    System.setProperty("hadoop.home.dir","C:/winutils")


    //Schema for POIs dataset
    var schema = ScalaReflection.schemaFor[POI].dataType.asInstanceOf[StructType]
    val poisDS1 = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/POIs.csv")
      .as[POI]

    //Sample 10% of the data

    val poisDS=poisDS1.sample(false,0.1)
    poisDS.show(10)

    //Build RTree for POIs dataset
   val poi_index= poisDS.index(RTreeType, "poisRT", Array("poi_lon", "poi_lat"))

    poisDS.printSchema


    //Find the coordinates of the MBRs of each partition
   val wlist= poi_index.mapPartitions { pois =>
     var min_lat=Double.MAX_VALUE
     var min_long=Double.MAX_VALUE
     var max_lat=Double.MIN_VALUE
     var max_long= Double.NEGATIVE_INFINITY


     while(pois.hasNext){
       val poi=pois.next()
       if(poi.poi_lat<min_lat){
         min_lat=poi.poi_lat
       }
       if(poi.poi_lat>max_lat){
         max_lat=poi.poi_lat
       }
       if(poi.poi_lon<min_long){
         min_long=poi.poi_lon
       }
       if(poi.poi_lon>max_long){
         max_long=poi.poi_lon
       }
     }

    List((min_long,min_lat, max_long, max_lat)).iterator

   }
    println("This is the lat and long values")
    wlist.show(10)

    //Write the coordinates of the MBRs in to the file
    wlist.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("Qplot_wlist_Answer1")
    poisDS.show(10)

    poi_index.
      write.
      format("com.databricks.spark.csv").option("header","true").save("Qplot_POI_Answer1")

    //Schema for Trajectories dataset
    schema = ScalaReflection.schemaFor[Car].dataType.asInstanceOf[StructType]
    val carsDS1 = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/trajectories.csv")
      .as[Car]

    //Sample 10% of the data
    val carsDS=carsDS1.sample(false,0.1)
    carsDS.show(10)

    //Build RTree for Trajectories dataset
   val carindex=carsDS.index(RTreeType, "carsRT", Array("car_lon", "car_lat"))
    carsDS.printSchema

    //Find the coordinates of the MBRs of each partition
    val wcarlist= carindex.mapPartitions { cars =>
      var min_lat=Double.MAX_VALUE
      var min_long=Double.MAX_VALUE
      var max_lat=Double.MIN_VALUE
      var max_long= Double.NEGATIVE_INFINITY

      
      while(cars.hasNext){
        val car=cars.next()
        if(car.car_lat<min_lat){
          min_lat=car.car_lat
        }
        if(car.car_lat>max_lat){
          max_lat=car.car_lat
        }
        if(car.car_lon<min_long){
          min_long=car.car_lon
        }
        if(car.car_lon>max_long){
          max_long=car.car_lon
        }
      }

      List((min_long,min_lat, max_long, max_lat)).iterator

    }
    println("For trajecotries")
    wcarlist.show(10)

    //Write the coordinates of the MBRs in to the file
    wcarlist.coalesce(1).write.format("com.databricks.spark.csv").option("header","true").save("Qplot_wcarlist_Answer1")

    carindex.
      write.
      format("com.databricks.spark.csv").option("header","true").save("Qplot_Traj_Answer1")
    simba.stop()

  }
}
