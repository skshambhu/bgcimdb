/**
*	Author: 	Sanjay Shambhu
*	Purpose:	This program contains the main function
*	How to use:	The program accepts one argument, which is name of the configuration
*				file. The program has been designed take almost all configuration
				as part of the config file including spark master node. A sample
				config file is located at ./src/main/resources/program.properties.
*/

package com.bgcpartners.imdb

import java.io.FileInputStream
import java.util.Properties

object ImdbAgg {
  
  def main(args: Array[String]): Unit = {
    val imdbConf = new Properties()
    imdbConf.load(new FileInputStream(args(0)))

    val rankings = new Rankings(imdbConf)

    // Call the process function in the Rankings class to start processing.
    rankings.process()
  }
}