package com.virtualpairprogrammers;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

	public static void main(String[] args) {
		
		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		final Logger logger = Logger.getLogger(Main.class);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StartingSpark");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		JavaRDD<Integer> myRDD = sc.parallelize(inputData);
		
		Integer result = myRDD.reduce( (value1, value2) -> value1 + value2);
		
		JavaRDD<Double> sqrtRdd = myRDD.map(value -> Math.sqrt(value));
		
		sqrtRdd.foreach(value -> System.out.println(value));
		
		System.out.println(result);
		
		sc.close();
		

	}

}
