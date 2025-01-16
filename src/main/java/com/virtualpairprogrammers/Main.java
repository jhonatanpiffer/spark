package com.virtualpairprogrammers;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {

		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StartingSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		JavaRDD<String> usefullLines = initialRdd.filter( word -> !(word.matches("^(\\d{2}:\\d{2}:\\d{2},\\d{3}|\\d+).*$") || word.matches("^\\s*$")) );
		JavaRDD<String> smallcase = usefullLines.map(word -> word.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> justInterestingWords = smallcase.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
				.filter( word -> Util.isNotBoring(word));
		
		JavaRDD<String> removeEmpty = justInterestingWords.filter( word -> word.trim().length() > 0);
		
		JavaPairRDD<String, Long> wordsHash = removeEmpty.mapToPair(word -> new Tuple2<>(word, 1L));
		JavaPairRDD<String, Long> result = wordsHash.reduceByKey( (number1, number2) -> number1 + number2);
		JavaPairRDD<Long, String> switched = result.mapToPair( tuple -> new Tuple2<Long, String> (tuple._2, tuple._1));
		JavaPairRDD<Long, String> sorted = switched.sortByKey(false);
		
		sorted.take(50).forEach(System.out::println); 

		sc.close();
	}

}
