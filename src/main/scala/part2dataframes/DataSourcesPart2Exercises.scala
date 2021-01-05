package part2dataframes

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{ SaveMode, SparkSession }

object DataSourcesPart2Exercises extends App with LazyLogging {
  /**
   * Exercises
   *
   * 1) Read the movies DataFrame, then write it as:
   *  - tab-separated values file
   *  - snappy Parquet format
   *  - table in the PostgreSQL database in a table called public.movies
   */
  val spark = SparkSession.builder
      .config( "spark.master", "local" )
      .config( "appName", "DataSources exercises" )
      .getOrCreate()
  
  val movies = spark.read
      .option( "inferSchema", "true" )
      .json( "src/main/resources/data/movies.json" )
  
  movies.show( true )
  
  // Storing DataFrame as Tab-separated values file
  logger.info( "Storing DataFrame as Tab-separated values file" )
  movies.write
      .format( "csv" )
      .mode( SaveMode.Overwrite )
      .option( "sep", "\t" )
      .option( "header", "true" )
      .save( "src/main/resources/data/output/movies.tsv" )
  
  // Storing DataFrame as Snappy-Parquet format
  logger.info( "Storing DataFrame as Snappy-Parquet format" )
  movies.write
      .format( "parquet" )
      .mode( SaveMode.Overwrite )
      .option( "compression", "snappy" )
      .save( "src/main/resources/data/output/movies.parquet" )
  
  logger.info( "Reading the DataFrame from the Snappy-Parquet format file we have just written a moment ago ..." )
  spark.read
      .format( "parquet" )
      .option( "compression", "snappy" )
      .parquet( "src/main/resources/data/output/movies.parquet" )
      .show()
  
  // Storing DataFrame as a PostgreSQL table
  logger.info( "Storing DataFrame as a PostgreSQL table" )
  movies.write
      .format( "jdbc" )
      .option( "driver", "org.postgresql.Driver" )
      .option( "url", "jdbc:postgresql://localhost:5432/rtjvm" )
      .option( "user", "docker" )
      .option( "password", "docker" )
      .option( "dbtable", "public.movies" )
      .save()
  
}
