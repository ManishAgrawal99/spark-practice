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
		
		//Data in List
		List<Tuple2<Integer, Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<>(4,18));
		visitsRaw.add(new Tuple2<>(6,4));
		visitsRaw.add(new Tuple2<>(10,9));
		
		List<Tuple2<Integer, String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<>(1,"John"));
		usersRaw.add(new Tuple2<>(2,"Bob"));
		usersRaw.add(new Tuple2<>(3,"Alan"));
		usersRaw.add(new Tuple2<>(4,"Dorris"));
		usersRaw.add(new Tuple2<>(5,"Mary"));
		usersRaw.add(new Tuple2<>(6,"Jane"));
		
		//Importing Data to RDDs
		JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);
		
		//Applying Inner Joins
	 	JavaPairRDD<Integer, Tuple2<Integer, String>> joinedRdd = visits.join(users);
	 	
	 	joinedRdd.foreach(pair -> System.out.println("User Id: " + pair._1 + " name: " + pair._2._2 + " visits: " + pair._2._1));
		
		sc.close();
	}

}
