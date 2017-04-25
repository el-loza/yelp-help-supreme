//mvn clean compile package
//"$SPARK_HOME"/bin/spark-submit --class "temp" --master spark://kinshasa:7077 target/temp-0.0.1.jar

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple5;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.col;


public class BusinessMonthlyRating {

	public static SparkSession spark; 


	public static void main(String[] args) {
		try {
		
		spark = SparkSession
				.builder()
				.appName("testSpark")
				.enableHiveSupport()	
				.getOrCreate();
		//******Populating dataset for review**************************************************************************************************************
		Dataset<Row> dsReview = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_review.json");
		
		Dataset<Row> reviews = dsReview
				.select(col("review_id"), col("user_id").as("usr_rev_id"), col("business_id"),col("stars"), functions.year(col("date")).as("year"),functions.month(col("date")).as("month"))  
				.where(col("user_id").isNotNull());
		reviews.show(100);
		System.out.println("All reviews!!!!");
		
//****** Map/Reduce Review**************************************************************************************************************
		JavaRDD<Row> reviewsRDD = reviews.toJavaRDD();
		JavaPairRDD<String, Tuple2<Float,Long>> RestaurantPerMonthRaw = reviewsRDD.mapToPair((Row row) -> {
			
			int year = Integer.parseInt(row.get(4).toString());
			int month = Integer.parseInt(row.get(5).toString());
			
			int monthYear = (year * 12) + month; 
			
			
			String key = row.get(2).toString() + "\",\"monthYear\":\"" + monthYear + "\",";
			Float stars = Float.parseFloat(row.get(3).toString());
			long count = 1;
			return new Tuple2<>(key, new Tuple2<>(stars,count));
		}).reduceByKey((Tuple2<Float,Long> t1 , Tuple2<Float,Long> t2)->
		new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));
		
		JavaPairRDD<String,Float> RestaurantPerYear = RestaurantPerMonthRaw.mapValues((Tuple2<Float,Long> t1) ->
		(t1._1 / (float) t1._2));
		
		PrintWriter pwriter = new PrintWriter("RestaurantPerMonthlyYearRating.json","UTF-8");
		List<Tuple2<String,Float>> answers = RestaurantPerYear.collect();
		for(Tuple2<String,Float> answer : answers){
			//pwriter.println(answer._1 + ", " + answer._2);
			pwriter.println("{\"bid\":\"" + answer._1 + "\"rating\":\"" + answer._2 + "\"}");

		}
		pwriter.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
