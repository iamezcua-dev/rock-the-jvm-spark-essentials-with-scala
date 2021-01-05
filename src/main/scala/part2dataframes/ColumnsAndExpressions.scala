package part2dataframes

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{ col, column, expr }

object ColumnsAndExpressions extends App with LazyLogging {
  val spark = SparkSession.builder
      .appName( "DataFrame Columns and Expressions" )
      .config( "spark.master", "local" )
      .getOrCreate()
  logger.debug( "Created Spark Session instance" )
  
  val carsDF = spark.read
      .option( "inferSchema", "true" )
      .json( "src/main/resources/data/cars.json" )
  carsDF.show( false )
  
  // Columns
  logger.info( "Getting a Column reference" )
  val firstColumn = carsDF.col( "Name" )
  println( firstColumn.toString() )
  
  // Selecting ( projection )
  logger.info( "Selecting ( making a projection ) the data referenced by the `firstColumn` Column ..." )
  val carNamesDF = carsDF.select( firstColumn )
  carNamesDF.show( false )
  
  // Various select methods
  logger.info( "Projecting data by using different ways: DataFrame col method call, SparkSQL col function, SparkSQL column function, " +
      "Implicit conversion from Symbol to Column, Implicit conversion from Interpolated String to Column and using " +
      "Expressions." )
  import spark.implicits._
  
  carsDF.select(
    carsDF.col( "Name" ),
    col( "Acceleration" ),
    column( "Weight_in_lbs" ),
    'Year, // Scala Symbol, autoconverted to Column
    $"Horsepower", // Fancier interpolated String, returns a Column Object
    expr( "Origin" ) // Expression
  ).show
  
  // Select with plain column names
  logger.info( "Selecting data using plain column names" )
  carsDF.select( "Name", "Year" ).show
  
  // Expressions
  val simplestExpression = carsDF.col( "Weight_in_lbs" )
  val weightInKgExpression = carsDF.col( "Weight_in_lbs" ) / 2.2
  
  logger.info( "Selecting data using Expressions and its variants ..." )
  val carsWithWeightsDF = carsDF.select(
    col( "Name" ),
    col( "Weight_in_lbs" ),
    weightInKgExpression.as( "Weight_in_kg" ),
    expr( "Weight_in_lbs / 2.2" ).as( "Weight_in_kg_2" )
  )
  carsWithWeightsDF.show
  
  // selectExpr
  logger.info( "Selecting data using selectExpr method ..." )
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "(Weight_in_lbs / 2.2) AS Weight_in_kg"
  )
  carsWithSelectExprWeightsDF.show
  
  // DataFrame processing
  
  // Adding a column
  logger.info( "Adding a Column ..." )
  val carsWithKg3DF = carsDF.withColumn( "Weight_in_kg_3", col( "Weight_in_lbs" ) / 2.2 )
  carsWithKg3DF.show
  
  // Renaming a Column
  logger.info( "Renaming a Column ..." )
  val carsWithColumnRenamed = carsDF.withColumnRenamed( "Weight_in_lbs", "Weight in pounds" )
  carsWithColumnRenamed.show
  
  // Note: Be careful with column names
  logger.info( "Selecting data with SelectExpr and being careful with column names ( using backticks to allow spaces in Column names )" )
  carsWithColumnRenamed.selectExpr( "`Weight in pounds`" ).show
  
  // Removing a column
  logger.info( "Dropping two columns ..." )
  carsWithColumnRenamed.drop( "Cylinders", "Displacement" ).show
  
  // Filtering
  logger.info( "Filtering data ..." )
  val europeanCarsDF = carsDF.filter( col( "Origin" ) =!= "USA" )
  val europeanCarsDF2 = carsDF.where( col( "Origin" ) =!= "USA" )
  
  // Filtering with expressions Strings
  logger.info( "Filtering with expressions Strings" )
  val americanCarsDF = carsDF.filter( "Origin = 'USA'" )
  
  // Chain filter
  logger.info( "Chaining filtering operations" )
  val americanPowerfulCarsDF = carsDF.filter( col( "Origin" ) === "USA" ).filter( col( "Horsepower" ) > 150 )
  val americanPowerfulCarsDF2 = carsDF.filter( col( "Origin" ) === "USA" and col( "Horsepower" ) > 150 )
  val americanPowerfulCarsDF3 = carsDF.filter( "Origin = 'USA' and Horsepower > 150" )
  
  // Unioning ( adding more rows )
  logger.info( "Unioning two DataFrames that share the same schema" )
  val moreCarsDF = spark.read.option( "inferSchema", "true" ).json( "src/main/resources/data/more_cars.json" )
  val allCarsDF = carsDF.union( moreCarsDF ) // Works if the DataFrames have the same schema
  allCarsDF.show
  
  // Distinct values
  logger.info( "Extracting distinct countries" )
  val allCountriesDF = carsDF.select( "Origin" ).distinct()
  allCountriesDF.show
  
}
