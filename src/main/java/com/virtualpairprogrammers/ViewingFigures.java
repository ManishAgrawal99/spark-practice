package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

public class ViewingFigures 
{
	@SuppressWarnings("resource")
	public static void main(String[] args)
	{
		
		//Setting Up the project
		System.setProperty("hadoop.home.dir", "c:/STUDY/hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		
		
		
		//Fetching data into RDDs from methods
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);
		
		
		

		// Number of chapters in each course
		JavaPairRDD<Integer, Integer> swappedChapterData = chapterData.mapToPair(pair -> new Tuple2<Integer, Integer>(pair._2,1));
		JavaPairRDD<Integer, Integer> countRdd = swappedChapterData.reduceByKey((value1, value2) -> value1+value2);
		//countRdd.foreach(pair -> System.out.println("CourseId: " + pair._1 + " Chapters: " + pair._2));
		
		
		
		
		//Removing Duplicated Views
		viewData = viewData.distinct();
		//viewData.foreach(view -> System.out.println(view));
		
		
		
		
		//Joining ViewData and ChapterData
		JavaPairRDD<Integer, Integer> swappedViewData = viewData.mapToPair((pair)-> new Tuple2<Integer, Integer>(pair._2, pair._1));
		JavaPairRDD<Integer, Tuple2<Integer, Integer>> joinedData = swappedViewData.join(chapterData);
		//joinedData.foreach(pair -> System.out.println(pair));
		//Data of form <ChapterID, (UserID, CourseID)>
		
		
		//Now ChapterId is not useful
		//We just want the count of number of chapters in a course viewed by a user
		//We have a userId and courseId tuple and we need to count them
		JavaPairRDD<Tuple2<Integer, Integer>, Long> reversedJoinedData = joinedData.mapToPair((pair)-> new Tuple2<Tuple2<Integer, Integer>, Long>(pair._2, 1L));
		JavaPairRDD<Tuple2<Integer, Integer>, Long> userCourseChaptersViewedCount = reversedJoinedData.reduceByKey((value1, value2)-> value1+value2);
		//userCourseChaptersViewedCount.foreach((pair)-> System.out.println(pair));
		//Data of form <(userID, CourseID), ChaptersViewedCount>
		
		
		
		//Now that we don't need the info on user we can drop it
		JavaPairRDD<Integer, Long> courseViewedCount = userCourseChaptersViewedCount.mapToPair(row -> new Tuple2<Integer, Long>(row._1._2, row._2));
		//courseViewedCount.foreach(row -> System.out.println(row));
		//Data of form <CourseId, ChaptersViewedCount>
		
		
		
		//Getting ratio of chapters watched
		JavaPairRDD<Integer, Tuple2<Long, Integer>> chaptersViewedOf =  courseViewedCount.join(countRdd);
		//chaptersViewedOf.foreach(row -> System.out.println(row));
		//Data of type <CourseID, (chaptersViewed, totalChapters)>
		
		
		
		//Converting to percentages
		JavaPairRDD<Integer, Double> ratioedViews = chaptersViewedOf.mapValues(value -> (double) value._1/value._2);
		//ratioedViews.foreach(row-> System.out.println(row));
		//Data of Type <CourseId, ratio of watched chapters>
		
		
		//Converting to Scores
		JavaPairRDD<Integer, Long> individuallyScored = ratioedViews.mapValues(value -> {
			if(value > 0.9) return 10L;
			if(value > 0.5) return 4L;
			if(value > 0.25) return 2L;
			else return 0L;
		});
		//individuallyScored.foreach(row-> System.out.println(row));
		//Data of type <CourseID, score>
		
		
		
		//Getting totaled Scores
		JavaPairRDD<Integer, Long> courseScores = individuallyScored.reduceByKey((value1, value2)-> value1+value2);
		//courseScores.foreach(row-> System.out.println(row));
		//Data of type <CourseID, score>
		
		
		//Getting the course title along
		JavaPairRDD<Integer, Tuple2<Long, String>> titledScores = courseScores.join(titlesData);
		JavaPairRDD<String, Long> finalScores = titledScores.mapToPair(row -> new Tuple2<String, Long>(row._2._2, row._2._1));
		finalScores.foreach(row-> System.out.println(row));
		
		
		
		
		sc.close();
	}

	
	
	
	
	
	
	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (courseId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));
			rawChapterData.add(new Tuple2<>(99,  2));
			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode)
		{
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(14, 99));
			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}
		
		return sc.textFile("src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
