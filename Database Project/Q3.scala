
/**
  * Created by prdin on 05-Jun-17.
  */

import java.io._

import org.apache.spark.rdd._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.simba.index.RTreeType

object Q3 {
  case class POI(pid: Long, tags_poi: String, poi_lon: Double, poi_lat: Double)
  case class Car(cid:Long, sid: Long, car_lon: Double, car_lat: Double, days: String, times: String)
  case class Centeroids(center_lon: Double, center_lat: Double)

  def main(args: Array[String]): Unit = {

    val start_time=System.currentTimeMillis()
    val simba = SimbaSession.
      builder().
      master("local[*]"). //Change * to 1 for executing the program using 1 core
      appName("Q3").
      config("simba.index.partitions", "8").
      getOrCreate()

    import simba.implicits._
    import simba.simbaImplicits._

    simba.sparkContext.setLogLevel("ERROR")
    System.setProperty("hadoop.home.dir","C:/winutils")

    //The area of coordinates (-332729.310,4456050.000) and (-316725.862,4469518.966) is divided into 500 x 500-meters grids

7
    val sc=simba.sparkContext
    val longs= (-332729.310 to -316725.862 by 500)
    val lats = (4456050.000 to 4469518.966 by 500)

    val rddlongs=sc.parallelize(longs)
    val rddlats=sc.parallelize(lats)

    //Do cartesian join of the coordinates
    val rddscar=rddlongs.cartesian(rddlats).toDF("cell_long","cell_lat")
    rddscar.show(10)




    var schema = ScalaReflection.schemaFor[Car].dataType.asInstanceOf[StructType]
    val carsDS = simba.read
      .option("header", "true")
      .schema(schema)
      .csv("datasets/trajectories.csv")
      .as[Car]




    carsDS.show(10)
    //Build index for trajectories dataset
    val cari=carsDS.index(RTreeType, "carsRT", Array("car_lon", "car_lat"))
    carsDS.printSchema

    //Consider only trajectories in the grid covered by the given coordinates
    val carsDS1=cari.range(Array("car_lon","car_lat"), Array(-332729.310,4456050.000), Array(-316725.862,4469518.966))

    //Perform knnjoin between Trajectory dataset and cartesian dataset
   val carsknn=carsDS1.knnJoin(rddscar,Array("car_lon", "car_lat"), Array("cell_long", "cell_lat"),1)


    //Count the trajectories around POIs
     val x=carsknn.groupBy("cell_long","cell_lat").count()
     val x_sorted=x.sort($"count".desc)
    println("Aggregate per cell in a 500 x 500 meters grid")
    x_sorted.show(10)



    print("Time Taken exculding file write is : ")
    println(System.currentTimeMillis()-start_time)

    //Save the results in carsknn1.csv
    //The results are refined and the number of trajectory points in each cell are extracted.
    //The final results are in Q3_answers.csv
     x_sorted.coalesce(1).
      write.
      format("com.databricks.spark.csv").option("header","true").save("Q3_final_answer_1")

    simba.stop()

  }
}
