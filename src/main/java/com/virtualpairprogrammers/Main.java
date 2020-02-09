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
		inputData.add("WARN: Tuesday 4 September 0408");
		inputData.add("WARN: Wednesday 5 September 1632");
		inputData.add("WARN: Thursday 7 September 1854");
		inputData.add("WARN: Friday 8 September 1942");
		
		//Setting up Spark
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Importing the data into RDDs from List
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
		
		JavaPairRDD<String, String> pairRdd = originalLogMessages.mapToPair(rawValue -> {
			String[] pairs = rawValue.split(":");
			String levels = pairs[0];
			String date = pairs[1];
			return new Tuple2<String, String>(levels, date);
		});
		
		sc.close();
	}

}
