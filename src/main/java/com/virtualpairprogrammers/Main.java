package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.Arrays;
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
		JavaRDD<String> sentences = sc.parallelize(inputData);
		
		//Converting the sentences RDD into a RDD containing individual words
		JavaRDD<String> words = sentences.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
		JavaRDD<String> filteredWords = words.filter(word -> word.length()>1);
		
		filteredWords.foreach(word-> System.out.println(word));
		
		sc.close();
	}

}
