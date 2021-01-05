package part2dataframes

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, expr, when }

object ColumnsAndExpressionsExercises extends App with LazyLogging {
  /**
   * Exercises
   */

  val spark = SparkSession.builder
      .appName( "Columns and Expressions Exercises" )
      .config( "spark.master", "local" )
      .getOrCreate()
  
  val moviesDF = spark.read.json( "src/main/resources/data/movies.json" )

  /**
   * 1. Read the movies Dataframe and select 2 columns of your choice.
   */
  logger.info( "1) Read the movies Dataframe and select 2 columns of your choice." )
  import spark.implicits._
  moviesDF.select(
    $"Title",
    'IMDB_Rating
  ).sort( 'IMDB_Rating.desc ).show( false )
  
  /**
   * 2. Create a new column by summing up the total profit of the movies = US_Gross + WorldWide_Gross + DVD_Sales
   */
  logger.info( "2. Create a new column by summing up the total profit of the movies = US_Gross + WorldWide_Gross + DVD_Sales" )
  val movieProfit = col( "US_Gross_C" ) + col( "Worldwide_Gross_C" ) + col( "US_DVD_Sales_C" )
  val moviesProfitDF = moviesDF
      .withColumn( "US_Gross_C", when( 'US_Gross.isNull, 0L ).otherwise( 'US_Gross ) )
      .withColumn( "Worldwide_Gross_C", when( 'Worldwide_Gross.isNull, 0L ).otherwise( 'Worldwide_Gross ) )
      .withColumn( "US_DVD_Sales_C", when( 'US_DVD_Sales.isNull, 0L ).otherwise( 'US_DVD_Sales ) )
      .withColumn( "Movie_Profit", movieProfit )
      .select(
        'Title,
        $"Director",
        'Distributor,
        expr( "Major_Genre" ),
        expr( "IMDB_Rating" ),
        $"Movie_Profit"
      ).sort( 'Movie_Profit.desc )
  moviesProfitDF.show( false )
  moviesProfitDF.printSchema
  
  /**
   * 3. Select all COMEDY movies with IMDB rating above 6
   */
  logger.info( "3. Select all COMEDY movies with IMDB rating above 6" )
  moviesProfitDF.filter( expr( "Major_Genre" ) === "Comedy" )
      .filter( col( "IMDB_Rating" ) > 6 )
      .sort( 'IMDB_Rating.desc )
      .show( false )
}