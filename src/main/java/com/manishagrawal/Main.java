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
		Dataset<Row> dataset = spark.read().option("header", true).csv("src/main/resources/biglog.txt");
		
		
		
		//Using aggregation function in Spark SQL
		//dataset.createOrReplaceTempView("logging_table");
		//Dataset<Row> results = spark.sql
		//		("select level, date_format(datetime, 'MMMM') as month, cast(first(date_format(datetime, 'M')) as int) as monthnum, count(1) as total "
		//		+ "from logging_table "
		//		+ "group by level, month order by monthnum, level"
		//			);
		//results.show(100);
		
		dataset = dataset.select(col("level"), 
								 date_format(col("datetime"), "MMMM").alias("month"), 
								 date_format(col("datetime"), "MM").alias("monthnum").cast(DataTypes.IntegerType)
								 );
		
		Object[] months = new Object[] {"January", "February", "March", "April", "May", "June", "July", "August", "Blala", "September", "October", "November", "December"};
		List<Object> columns = Arrays.asList(months);
		
		//Pivoting
		dataset = dataset.groupBy("level").pivot("month", columns).count().na().fill(0);
		
		dataset.show();
		
		spark.close();
		
		
	}

}