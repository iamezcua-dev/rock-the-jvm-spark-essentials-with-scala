package part2dataframes

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregationsExercises extends App with LazyLogging {
  /**
   * Exercises
   */
  
  /**
   * 1) Sum up ALL the profits of ALL the movies in the DF
   */
  val spark = SparkSession.builder
      .appName( "DataFrames Aggregations Exercises" )
      .config( "spark.master", "local" )
      .getOrCreate()
  
  val moviesDF = spark.read.json( "src/main/resources/data/movies.json" )
  
  logger.info( "1) Sum up ALL the profits of ALL the movies in the DF" )
  import spark.implicits._
  val usDvdSalesSum = sum( 'US_DVD_Sales )
  val worlwideGrossSum = sum( 'Worldwide_Gross )
  val usGrossSum = sum( 'US_Gross )
  moviesDF.select( ( usDvdSalesSum + worlwideGrossSum + usGrossSum ).as( "All_Profits" ) ).show()
  
  /**
   * 2) Count how many distinct directors we have
   */
  logger.info( "2) Count how many distinct directors we have" )
  moviesDF.select( countDistinct( 'Director ).as( "Distinct_Directors" ) ).show()
  
  /**
   * 3) Show the mean and standard deviation of US gross revenue for the movies
   */
  logger.info( "3) Show the mean and standard deviation of US gross revenue for the movies" )
  moviesDF.select(
    format_string( "%.10f", expr( "mean( US_Gross )" ) ).as( "Average_US_Gross_Revenue" ),
    format_string( "%.10f", stddev( 'US_Gross ) ).as( "Standard_Deviation_Of_US_Gross_Revenue" )
  ).show( false )
  
  /**
   * 4) Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
   */
  logger.info( "4) Compute the average IMDB rating and the average US gross revenue PER DIRECTOR" )
  moviesDF.groupBy( 'Director )
      .agg(
        sum( 'US_Gross ).as( "Total_US_Gross" ),
        avg( 'IMDB_Rating ).as( "Average_IMDB_Rating" )
      ).orderBy( 'Average_IMDB_Rating.desc_nulls_last ).show( false )
  
}
