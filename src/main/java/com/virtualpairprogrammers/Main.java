package com.virtualpairprogrammers;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StartingSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> initialRdd = sc.textFile("src/main/resources/subtitles/input.txt");
		
		JavaRDD<String> lettersOnly = initialRdd.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "").toLowerCase());
		
		JavaRDD<String> removedBlankLines = lettersOnly.filter(sentence -> sentence.trim().length() > 0 );
		
		JavaRDD<String> justWords = removedBlankLines.flatMap( words -> Arrays.asList(words.split(" ")).iterator() );
		
		JavaRDD<String> justInterestingWords =  justWords.filter(word -> Util.isNotBoring(word));
				
		List<String> results = justInterestingWords.take(50);
		results.forEach(System.out::println);
			
		
		sc.close();
	}

}
