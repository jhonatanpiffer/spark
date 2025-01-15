package com.virtualpairprogrammers;

import java.util.ArrayList;
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
				
		initialRdd
			.flatMap(value -> Arrays.asList(value.split(" ")).iterator())
			.filter(word -> word.length() > 1  )
			.foreach(value -> System.out.println(value));
			
		
		sc.close();
	}

}
