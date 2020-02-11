package com.manishagrawal;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		
		System.setProperty("hadoop.home.dir", "c:/STUDY/hadoop");
		//Setting up Spark
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
												   .config("spark.sql.warehouse.dir", "file:///c:/tmp/")
												   .getOrCreate();
		
		
		
		//Reading a CSV file
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/exams/students.csv");
		
		dataset.show();;
		//System.out.println(dataset.count() );
		
		
		//Filtering Rows with subject as Modern Art
		Dataset<Row> modernArtResults = dataset.filter("subject = 'Modern Art' AND year>=2007 ");
		
		//Setting up spark to use sql queries
		dataset.createOrReplaceTempView("my_students_table");
		Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
		
		
		results.show(); 
		
		spark.close();
		
		
	}

}