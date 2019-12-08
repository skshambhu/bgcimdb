/**
* Author:   Sanjay Shambhu
* Purpose:  This program hosts Ranking class for Analysing IMDB movie dataset
* How to use: Methods in this class is invoked from ImdbAgg.scala
              All external dependencies are pass via process method via a property
              file.

* Testing   : Corresponding test programs are available in test/scala/com/bcgpartners/
              imdb/RankingsTest.scala
              To run the test, please provide a similar config property file for 
              test data and configuration. A sample is available test/resources/
              testImdbConf.properties
              Five sample test data files are available at test/resources/data
*/

package com.bgcpartners.imdb

import java.util.Properties

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
  Analyse IMDB movie dataset
    1. Get top 20 movies based on number of ratings and average rating
    2. Get most often credited persons from top 20 movies
    3. Get all titles for the top 20 movies
*/

class Rankings(imdbConf: Properties) {

  //Initialize Spark Session
  val spark: SparkSession = SparkSession
    .builder
    .master(imdbConf.getProperty("master"))
    .appName(imdbConf.getProperty("appName"))
    .getOrCreate

  def process(): Unit = {

    //Input Files
    val ratingsFile = imdbConf.getProperty("title.ratings")
    val titleFile = imdbConf.getProperty("title.basics")
    val akasFile = imdbConf.getProperty("title.akas")
    val principalsFile = imdbConf.getProperty("title.principals")
    val namesFile = imdbConf.getProperty("name.basics")

    println(namesFile)
    //Output Files
    val top20Outfile = imdbConf.getProperty("top20Movies.tsv")
    val principalsOutfile = imdbConf.getProperty("principals.tsv")
    val allTitlesOutfile = imdbConf.getProperty("allTitles.tsv")
   
    //Get top 20 movies
    val top20DF: DataFrame = top20Movies(ratingsFile, titleFile).cache

    //Get principals of 20 movies
    val principalsDF: DataFrame = topPrincipals(top20DF, principalsFile, namesFile)

    //Get all titles
    val allTitlesDF: DataFrame = allTitles(top20DF, akasFile)

    //Write the output the files as defined in configuration
    writeDF(top20DF, top20Outfile)

    writeDF(principalsDF, principalsOutfile)

    
    writeDF(allTitlesDF, allTitlesOutfile)

    spark.stop()
  }

  def top20Movies(ratingsFile: String, titleFile: String): DataFrame = {
    import spark.implicits._
    // read title details file into a dataframe
    val titleDF = readFile(titleFile).select($"tconst".as("movieId"), $"primaryTitle", $"titleType")

    // read ratings file into a dataframe
    val allRatingsDF = readFile(ratingsFile).where($"numVOtes" > 50)

    // filter titles to choose only movies and join with ratings dataframe
    val ratingDF = titleDF
      .filter($"titleType"==="movie")
      .join(allRatingsDF, titleDF("movieID")===allRatingsDF("tconst"))
      .select(titleDF("movieID"), $"primaryTitle", $"numVotes", $"averageRating").distinct
      .cache //caching it because it is going to be used twice below

    // calculate avg number of votes
    val avgNumRatings = ratingDF
      .select(avg($"numVotes"))
      .first
      .getDouble(0)

    val top20DF = ratingDF
      .select($"movieId", $"primaryTitle", ($"numVotes" * $"averageRating" / avgNumRatings).as("score"))
      .sort(desc("score"))
      .limit(20)

    ratingDF.unpersist() // unpersist as we don't need it anymore

    top20DF
  }

  def topPrincipals(top20DF: DataFrame, principalsFile: String, namesFile: String): DataFrame = {
    import spark.implicits._

    val principalsDF = readFile(principalsFile)
      .select($"nconst".as("principalId"), $"tconst".as("movieId"))
      .cache

    val principalNamesDF = readFile(namesFile)
      .select($"nconst".as("principalId"), $"primaryName".as("name"))

    // principalsFilteredDF contains principalId, movieId for all persons credited in top 10
    val principalsFilteredDF = principalsDF
      .join(broadcast(top20DF), top20DF("movieId")===principalsDF("movieId"))
      .select($"principalId",top20DF("movieId"))
      .cache
     
    // create list of distinct principals in top 10 movies to be used as filter
    val distinctPrincipalsDF = principalsFilteredDF
      .select($"principalId")
      .distinct()
    
    // principalsCountDF contains principalId, num of credits across full dataset for persons credited in top 10
    val principalsCountDF = principalsDF
      .join(distinctPrincipalsDF, principalsDF("principalId")===distinctPrincipalsDF("principalId"))
      .select(principalsDF("principalId"), $"movieId")
      .groupBy("principalId")
      .agg(count($"movieId").as("credits"))
    
    
    // principalsWithCountsDF contains principalId, movieID, num of overall credits for all persons credited in top 10
    val principalsWithCountsDF = principalsCountDF
      .join(principalsFilteredDF, principalsCountDF("principalId")===principalsFilteredDF("principalId"))
      .select(principalsCountDF("principalId"), $"movieId", $"credits")
      .orderBy($"credits".desc)
    
    // create window to rank and get top ranked person from all movies
    val byTitleDesc = Window.partitionBy($"movieId")
      .orderBy($"credits".desc)

    val rankByCredits = rank().over(byTitleDesc)

    // Add rank to Principals DF
    val topRankedPrincipalsDF = principalsWithCountsDF
      .select($"movieId", $"principalId", $"credits", rankByCredits.as("rank"))
      .where($"rank"===1)
    
    // Get names of Principals
    val topRankedPrincipalsWithNamesDF = topRankedPrincipalsDF
      .join(principalNamesDF,topRankedPrincipalsDF("principalId")===principalNamesDF("principalId"))

    // join with top20DF to get movies with titles and most often credited principals in a single DF
    val top20MoviesWithTopPrincipalsDF = topRankedPrincipalsWithNamesDF
      .join(top20DF, topRankedPrincipalsWithNamesDF("movieId")===top20DF("movieId"))
      .select(top20DF("movieId"), $"primaryTitle", $"name")
      .coalesce(1)
  
    principalsDF.unpersist()

    top20MoviesWithTopPrincipalsDF
  }

  def allTitles(top20DF: DataFrame, akasFile: String): DataFrame = {
    import spark.implicits._
    // Get all titles for top 10 Movies

    val akasDF = readFile(akasFile).select($"titleId", $"title")

    val top20AllTitlesDF = akasDF
      .join(broadcast(top20DF), top20DF("movieId")===akasDF("titleId"))
      .select(top20DF("movieId"), $"title")
      .groupBy($"movieId")
      .agg(concat_ws(", ", collect_list($"title")).as("allTitles"))
      .distinct
      .coalesce(1)

    top20AllTitlesDF
  }

  def writeDF(dataFrame: DataFrame, fileName: String): Unit = {
    dataFrame.write
      .format("csv")
      .option("header", imdbConf.getProperty("header"))
      .option("delimiter", imdbConf.getProperty("delimiter"))
      .save(fileName)
  }

  def readFile(fileName: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", imdbConf.getProperty("header"))
      .option("delimiter", imdbConf.getProperty("delimiter"))
      .option("quote","")
      .load(fileName)
  }
}
