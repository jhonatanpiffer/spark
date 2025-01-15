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
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("StartingSpark");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
				
		JavaRDD<Integer> originalIntergers = sc.parallelize(inputData);
		JavaRDD<IntegetWithSquareRoot> sqrtRdd = originalIntergers.map(value -> new IntegetWithSquareRoot(value));
		
		sqrtRdd.foreach((IntegetWithSquareRoot n) -> System.out.printf("%d %f\n",n.getOrignalNumber(), n.getSqrt()));
		
		
		sc.close();
	}

}
