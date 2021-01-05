package part2dataframes

import org.apache.spark.sql.types._
import org.apache.spark.sql.{ SaveMode, SparkSession }

object DataSources extends App {
  val spark = SparkSession.builder
      .appName( "DataSources and Formats" )
      .master( "local" )
      .getOrCreate()
  
  val carsSchema = StructType(
    Array(
      StructField( "Name", StringType ),
      StructField( "Miles_per_Gallon", DoubleType ),
      StructField( "Cylinders", LongType ),
      StructField( "Displacement", DoubleType ),
      StructField( "Horsepower", LongType ),
      StructField( "Weight_in_lbs", LongType ),
      StructField( "Acceleration", DoubleType ),
      StructField( "Year", StringType ),
      StructField( "Origin", StringType )
    )
  )
  
  /*
    Reading a DataFrame:
      - format
      - schema or inferSchema = true
      - path
      - zero or more options
   */
  val carsDF = spark.read
      .format( "json" )
      .schema( carsSchema )
      .option( "mode", "failFast" ) // Drop malformed, permisive (default)
      .option( "path", "src/main/resources/data/cars.json" )
      .load()
  
  val carsDFWithOptionMap = spark.read
      .format( "json" )
      .options( Map(
        "mode" -> "FAILFAST",
        "path" -> "src/main/resources/data/cars.json",
        "inferSchema" -> "true"
      ) )
      .load()
  
  /*
    Writing DataFrames
      - format
      - save mode = overwrite, append, ignore, errorIfExists
      - path
      - zero or more options
   */
  carsDF.write
      .format( "json" )
      .mode( SaveMode.Overwrite )
      .save( "src/main/resources/data/cars_dupe.json" )
}
