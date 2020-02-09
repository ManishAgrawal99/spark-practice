package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		
		//The data we will import in SPARK RDDs
		List<Integer> inputData = new ArrayList<Integer>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		//Setting up Spark
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Importing the data into RDDs from List
		JavaRDD<Integer> myRdd = sc.parallelize(inputData);
		
		//Mapping the RDDs into their square roots
		JavaRDD<IntegerWithSquareRoot> sqrtRdd = myRdd.map((value)-> new IntegerWithSquareRoot(value));
		
		sc.close();
	}

}
