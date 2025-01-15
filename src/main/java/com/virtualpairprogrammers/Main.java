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
		List<Double> inputData = new ArrayList<>();
		inputData.add(35.5);
		inputData.add(12.49943);
		inputData.add(90.32);
		inputData.add(20.32);
		SparkConf conf = new SparkConf().setAppName("StartingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		
		
		JavaRDD<Double> myRDD = sc.parallelize(inputData);
		
		Double result = myRDD.reduce( (value1, value2) -> value1 + value2);
		System.out.println(resultd);
		
		sc.close();
		

	}

}
