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
		
		System.setProperty("hadoop.home.dir", "c:/STUDY/hadoop");
		//Setting up Spark
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Importing the data into RDDs
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		//Converting the sentences RDD into a RDD containing individual words
		JavaRDD<String> words = initialRdd.flatMap(value -> Arrays.asList(value.split(" ")).iterator());
		
//		JavaRDD<String> filteredWords = words.filter(word -> word.length()>1);
		
		words.foreach(word-> System.out.println(word));
		
		sc.close();
	}

}
