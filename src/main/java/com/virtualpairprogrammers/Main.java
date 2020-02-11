package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

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
		
		//Cleaning Data
		JavaRDD<String> lettersOnlyRdd =  initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		JavaRDD<String> removedBlankLines = lettersOnlyRdd.filter((sentence)->sentence.trim().length()>1);
		
		//Converting sentences to words
		JavaRDD<String> words = removedBlankLines.flatMap(sentence->Arrays.asList(sentence.split(" ")).iterator());
		
		//Filtering the words based on their uniqueness by using the Util.java helper class 
		// that takes another list of words that are commonly used and are not that unique 
		JavaRDD<String> interestingWords = words.filter((word)-> Util.isNotBoring(word) && word.length()>1);
		
		//Pairing the words with their frequencies
		JavaPairRDD<String, Long> pairRdd = interestingWords.mapToPair(word -> new Tuple2<String, Long>(word, 1L));
		JavaPairRDD<String, Long> countRdd = pairRdd.reduceByKey((value1, value2)-> value1+value2);
		
		JavaPairRDD<Long, String> switched = countRdd.mapToPair(tuple -> new Tuple2<Long, String>(tuple._2, tuple._1));
		
		//Sorting words by frequency
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		
		List<Tuple2<Long, String>> results = sorted.take(10);
		results.forEach(word-> System.out.println(word));
		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		scanner.close();
		
		sc.close();
	}

}