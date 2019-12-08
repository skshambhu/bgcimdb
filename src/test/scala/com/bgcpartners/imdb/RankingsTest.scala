package com.bgcpartners.imdb

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfter, FunSpec}
import com.github.mrpowers.spark.fast.tests.{DataFrameComparer, DatasetComparer}

class RankingsTest extends FunSpec with BeforeAndAfter with DataFrameComparer {

  val imdbConf = new Properties()

  before{
    //Create your data
        imdbConf.load(new FileInputStream("/Users/sanjay/Desktop/bgcimdb/bgcimdb4/bgcimdb/src/test/resources/testImdbConf.properties"))
  }

  describe("Test top 20 Movies"){
    it("Should have the following data"){
      val spark: SparkSession = SparkSession
        .builder
        .master(imdbConf.getProperty("master"))
        .appName(imdbConf.getProperty("appName"))
        .getOrCreate

      val expectedSchema = List(
        StructField("movieId", IntegerType, false),
        StructField("primaryTitle", StringType, false),
        StructField("name", StringType, false)
      )

      val expectedData = Seq(
        Row ("tt0000001", "Carmencita", "Fred Astaire"),
        Row("tt0000003", "Pauvre Pierrot", "Brigitte Bardot"),
        Row("tt0000002", "Le clown et ses chiens", "Lauren Bacall")
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val rankings = new Rankings(imdbConf)
      val actualDF = rankings.top20Movies(imdbConf.getProperty("title.ratings"),imdbConf.getProperty("title.basics"))

      //Testing expected output vs actual results
      assertSmallDatasetEquality(expectedDF, actualDF)

    }
  }

  describe("Test top principals"){
    it("Should have the following data"){

      val spark: SparkSession = SparkSession
        .builder
        .master(imdbConf.getProperty("master"))
        .appName(imdbConf.getProperty("appName"))
        .getOrCreate

      val expectedSchema = List(
        StructField("movieId", IntegerType, false),
        StructField("primaryTitle", StringType, false),
        StructField("score", FloatType, false)
      )

      val expectedData = Seq(
        Row ("tt0000001", "Carmencita", 1473.0847457627100),
        Row("tt0000003", "Pauvre Pierrot", 1253.0847457627100),
        Row("tt0000002", "Le clown et ses chiens", 192.3050847457630)
      )

      val expectedDF = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedData),
        StructType(expectedSchema)
      )

      val rankings = new Rankings(imdbConf)
      val top20DF = rankings.top20Movies(imdbConf.getProperty("title.ratings"),imdbConf.getProperty("title.basics"))
      val actualDF = rankings.topPrincipals(top20DF,imdbConf.getProperty("title.principals"),imdbConf.getProperty("name.basics"))
      assertSmallDatasetEquality(expectedDF, actualDF)
    }
  }

}
