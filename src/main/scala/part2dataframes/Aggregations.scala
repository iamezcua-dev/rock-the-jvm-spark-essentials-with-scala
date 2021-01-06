package part2dataframes

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ stddev, _ }

object Aggregations extends App with LazyLogging {
  val spark = SparkSession.builder
      .appName( "Aggregations and Grouping" )
      .config( "spark.master", "local" )
      .getOrCreate()
  
  spark.conf.set( "spark.sql.debug.maxToStringFields", "100" )
  
  logger.info( "Reading DataFrame from Movies JSON file ..." )
  val moviesDF = spark.read
      .option( "inferSchema", "true" )
      .json( "src/main/resources/data/movies.json" )
  
  import spark.implicits._
  // Counting
  // All the Major_Genres excepting nulls
  logger.info( "Counting the number of non-null Major Genres that exist in this DataFrame ..." )
  val genresCountDF = moviesDF.select( count( 'Major_Genre ).as( "All_Major_Genres_without_nulls" ) )
  moviesDF.selectExpr( "count(Major_Genre) AS All_Major_Genres_without_nulls" ).show()
  
  // Counting all rows ( this includes null Rows )
  logger.info( "Obtaining the number of rows in this DataFrame, including nulls ..." )
  moviesDF.select( count( '* ).as( "All_rows_count" ) ).show()
  
  // Count distinct
  logger.info( "Obtaining the number of distinct Major Genres and showing them for comparison purposes ..." )
  moviesDF.select( countDistinct( 'Major_Genre ).as( "Distinct_Major_Genres_count" ) ).show()
  moviesDF.select( 'Major_genre ).distinct.show()
  
  // Approximate count ( useful for large datasets )
  logger.info( "Getting the approximate count of Major Genres ( useful in very very large DataSets ) ..." )
  moviesDF.select( approx_count_distinct( 'Major_Genre ).as( "Approx_Major_Genre_Count" ) ).show()
  
  // Min and Max
  logger.info( "Getting the minimum and maximum IMDB rating ..." )
  val minAndMaxMoviesImdbRatingDF = moviesDF.select( min( 'IMDB_Rating ), max( 'IMDB_Rating ) )
  moviesDF.selectExpr( "min(IMDB_RATING)", "max(IMDB_Rating)" ).show()
  
  // Sum
  logger.info( "Getting the sum of the gross income in US ..." )
  val usGrossSumDF = moviesDF.select( sum( 'US_Gross ) )
  moviesDF.selectExpr( "sum(US_Gross)" ).show()
  
  // Avg
  logger.info( "Getting the average Rotten Tomatoes Rating ..." )
  val rottenTomatoesAverageRatingDF = moviesDF.select( avg( 'Rotten_Tomatoes_Rating ) )
  moviesDF.selectExpr( "avg(Rotten_Tomatoes_Rating)" ).show()
  
  // Data Science
  logger.info( "Showing the mean (average) and the standard deviation (stddev or stdev_samp) of the Rotten Tomatoes Rating ..." )
  moviesDF.select(
    mean( 'Rotten_Tomatoes_Rating ),
    stddev( 'Rotten_Tomatoes_Rating )
  ).show()
  
}
