package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		
		//The data we will import in SPARK RDDs
		List<String> inputData = new ArrayList<String>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Thursday 7 September 1854");
		inputData.add("WARN: Friday 8 September 1942");
		
		//Setting up Spark
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Importing the data into RDDs from List
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
		
		JavaPairRDD<String, Long> pairRdd = originalLogMessages.mapToPair(rawValue -> {
			String[] pairs = rawValue.split(":");
			String levels = pairs[0];
			return new Tuple2<String, Long>(levels, 1L);
		});
		//Printing to look how the data is arranged
		pairRdd.foreach(value -> System.out.println(value));
		
		JavaPairRDD<String, Long> countRdd = pairRdd.reduceByKey((value1, value2)->value1 + value2);
		countRdd.foreach(tuple -> System.out.println(tuple._1 + ": "+tuple._2));
		
		
		sc.close();
	}

}
