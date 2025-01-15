package com.virtualpairprogrammers;

import java.util.ArrayList;
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
		
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StartingSpark");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		JavaRDD<String> originalLogMessages = sc.parallelize(inputData);
		JavaPairRDD<String, Long>  pairRdd = originalLogMessages.mapToPair(rawvalue -> {
			String[] columns = rawvalue.split(":");
			String level = columns[0];
			
			return new Tuple2<>(level, 1L);
		});

		JavaPairRDD<String, Long> sumsRdd = pairRdd.reduceByKey( (value1, value2) -> value1 + value2);
		
		sumsRdd.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " instances"));
		
		sc.close();
	}

}
