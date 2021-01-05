package part2dataframes

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{ SaveMode, SparkSession }
import org.apache.spark.sql.types._
import part2dataframes.DataSources.{ carsSchema, spark }

object DataSourcesPart2 extends App with LazyLogging {
  val spark = SparkSession.builder
      .config( "spark.master", "local" )
      .appName( "DataSources and Formats" )
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
      StructField( "Year", DateType ),
      StructField( "Origin", StringType )
    )
  )
  // JSON Flags
  spark.read
      .schema( carsSchema )
      .option( "mode", "permissive" )
      .option( "dateFormat", "yyyy-MM-dd" ) // couple with schema; if Spark fail parsing, it will put null
      .option( "allowSingleQuotes", "true" )
      .option( "compression", "uncompressed" ) // bzip2, gzip, lz4, snappy, deflate
      .json( "src/main/resources/data/cars.json" ).show( false )
  
  // CSV Flags
  logger.info( "Reading a DataFrame from a CSV file" )
  val stocksSchema = StructType(
    Array(
      StructField( "Symbol", StringType ),
      StructField( "Date", DateType ),
      StructField( "Price", DoubleType )
    )
  )
  
  spark.read
      .schema( stocksSchema )
      .option( "dateFormat", "" )
      .option( "header", "true" )
      .option( "sep", "," )
      .option( "nullValue", "" )
      .csv( "src/main/resources/data/stocks.csv" )
  
  // Parquet
  logger.info( "Writing a DataFrame in Apache Parquet format" )
  logger.debug( "Firstly, reading a DataFrame from a JSON file" )
  val carsDF = spark.read
      .format( "json" )
      .schema( carsSchema )
      .option( "mode", "failFast" ) // Drop malformed, permisive (default)
      .option( "path", "src/main/resources/data/cars.json" )
      .load()
  
  logger.debug( "Now, we can write the DataFrame contents in Apache Parquet format ( this is the default format )" )
  carsDF.write
      .mode( SaveMode.Overwrite )
      .save( "src/main/resources/data/cars.parquet" )
  
  // Text files
  logger.info( "Reading a DataFrame from a plain-text file" )
  spark.read
      .text( "src/main/resources/data/sampleTextFile.txt" )
      .show( false )
  
  // Reading from a remote Database
  logger.info( "Reading a DataFrame from a Postgres Database" )
  val employees = spark.read
      .format( "jdbc" )
      .option( "driver", "org.postgresql.Driver" )
      .option( "url", "jdbc:postgresql://localhost:5432/rtjvm" )
      .option( "user", "docker" )
      .option( "password", "docker" )
      .option( "dbtable", "public.employees" )
      .load()
  
  employees.show()
}
