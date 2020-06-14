package com.leseanbruneau.streaming;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class LogStreamAnalysis {

	public static void main(String[] args)  {
		
		//System.setProperty("hadoop.home.dir", "c:/hadoop");	
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage").setLevel(Level.ERROR);
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("startingSpark");
		
		JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.seconds(2));
		
		JavaReceiverInputDStream<String> inputData = sc.socketTextStream("localhost", 8989);
		
		JavaDStream<String> results = inputData.map(item -> item);
		//results = results.map(rawLogMessage -> rawLogMessage.split(",")[0]);
		JavaPairDStream<String,Long > pairDStream = results.mapToPair(rawLogMessage -> new Tuple2<> (rawLogMessage.split(",")[0],1L));
		
		//pairDStream = pairDStream.reduceByKey((x,y) -> x + y);
		pairDStream = pairDStream.reduceByKeyAndWindow((x,y) -> x + y, Durations.minutes(2));
		
		//show first ten elements
		//results.print();
		pairDStream.print();
		
		sc.start();
		try {
			sc.awaitTermination();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
