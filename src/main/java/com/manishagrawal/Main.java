package com.manishagrawal;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import java.util.List;

public class Main {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/STUDY/hadoop");
		//Setting up Spark
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
												   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
												   .getOrCreate();
		
		//Reading the log file
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset = dataset.groupBy("subject").pivot("year").agg( round( avg(col("score")) , 2).alias("avg"),
																round( stddev(col("score")) , 2).alias("std dev")
															);
		
		dataset.show();
		
		spark.close();
		
		
	}

}