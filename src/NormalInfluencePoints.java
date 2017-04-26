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


public class NormalInfluencePoints {

	public static SparkSession spark; 


	public static float getScaling(double elite, double after)
	{
		double equalBuffer = 0.2;
		double withinBuffer = 0.5;
		float diff = (float) Math.abs(after - elite);

		if(diff < equalBuffer)
		{
			return 1;
		}
		else if((diff - equalBuffer) > withinBuffer)
		{
			return 0;
		}
		else
		{
			return (float) ((withinBuffer - (diff - equalBuffer) )/withinBuffer);
		}

	}

	//Determined if the elite rating matches the restaurant yearly trends
	public static float getTrend(double rating, double before, double after, double buffer)
	{
		if(Math.abs(rating - before) < buffer){
			if(Math.abs(rating - after) < buffer){
				return 1;
			}
			else{
				return 0;
			}
		}
		else if ( rating < before){
			if (rating < after){
				return 0;
			}
			else{
				return 1;
			}
		}
		else if (rating > before){
			if (rating > after){
				return 0;
			}
			else{
				return 1;
			}	
		}
		return 0;
	}
	public static void main(String[] args) {
		try {

			spark = SparkSession
					.builder()
					.appName("testSpark")
					.enableHiveSupport()	
					.getOrCreate();
			
			
			//******Populating dataset for user**************************************************************************************************************
			Dataset<Row> dsUsers = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_user.json");
			Dataset<Row> users = dsUsers.select(col("user_id"))
						.where(functions.array_contains(col("elite"), "None"))
						.limit(47343);//smaller dataset with user_id and elite years

			//******Populating dataset for review**************************************************************************************************************
			Dataset<Row> dsReview = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_review.json");

			Dataset<Row> reviews = dsReview
					.select(col("review_id"), col("user_id").as("usr_rev_id"), col("business_id"),col("stars"), functions.year(col("date")).as("year"))
					.where(col("user_id").isNotNull());

			reviews.show(10);
			System.out.println("All reviews!!!!");


			Dataset<Row> normalReivews = reviews
					.select(col("usr_rev_id"), col("business_id"),col("stars"), col("year").as("normal_year"))
					.join(users,col("user_id").equalTo(col("usr_rev_id")));
			normalReivews.show(10);
			System.out.println("Normal user reviews! >:D >_<");
			Dataset<Row> restaurantYearRating = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/RestaurantPerYearRating.json");


			// works up to here
			restaurantYearRating.show(10);

			System.out.println("first join");
			Dataset<Row> normalReivewsBefore = normalReivews
					.join(restaurantYearRating, col("business_id").equalTo(col("bid")).and(col("year").equalTo(col("normal_year").$minus(1))),"left_outer");
			normalReivewsBefore.show(10);
			
			//Rename
			Dataset<Row> normalReivewsBeforeRename = normalReivewsBefore
					.select(col("user_id"), col("business_id"), col("stars"), col("normal_year"), col("rating").as("beforerating"));
			normalReivewsBeforeRename.show(10);
			
			//Second join
			Dataset<Row> normalReviewBeforeAfter = normalReivewsBeforeRename
					.join(restaurantYearRating, col("business_id").equalTo(col("bid")).and(col("year").equalTo(col("normal_year").$plus(1))),"left_outer");
			normalReviewBeforeAfter.show(10);
			System.out.println("bar renamed");
			Dataset<Row> normalReviewBARename = normalReviewBeforeAfter
					.select(col("user_id"), col("business_id"), col("stars"), col("normal_year"), col("beforerating"), col("rating").as("afterrating"))
					.where(col("afterrating").isNotNull().and(col("beforerating").isNotNull()));
			normalReviewBARename.show(10);
			
//			eliteReviewBARename.write().json("EliteReviewBeforeAfter.json");
			
			System.out.println("first join");
			
			System.out.println("second join");

			System.out.println("------------------------------all tables created");
			
			System.out.println("************************************************************************************************************************");
			System.out.println("************************************************************************************************************************");
			System.out.println("reducing finished");
			System.out.println("************************************************************************************************************************");
			System.out.println("************************************************************************************************************************");

			//****** Map/Reduce Review**************************************************************************************************************
			JavaRDD<Row> eliteReviewsBA = normalReviewBARename.toJavaRDD();
			JavaPairRDD<String, Tuple2<Float,Long>> beforeAfter = eliteReviewsBA.mapToPair((Row row) -> {
				String key = row.get(0).toString();
				//rating - rating for this review. get(2)
				//beforeRating - rating for the restaurant the year before elite user rating - get(4) 
				//afterRating - rating for the restaurant the year after elite user rating - get(5)
				float equalBuffer = (float) 0.2;
				if(row.get(4) == null || row.get(5) == null || row.get(2) == null)
				{
					return null;
				}
		
				float before = Float.parseFloat(row.get(4).toString());
				float after = Float.parseFloat(row.get(5).toString());
				float rating = Float.parseFloat(row.get(2).toString());

				float weight = (float) 0.5;
				float influence = (weight * getTrend(rating, before, after, equalBuffer)) + ((1 - weight) * getScaling(rating, after));
				long count = 1;

				return new Tuple2<>(key, new Tuple2<>(influence,count));
			}).reduceByKey((Tuple2<Float,Long> t1 , Tuple2<Float,Long> t2)->
			new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));


			System.out.println();
			System.out.println();
			System.out.println("reducing finished");
			System.out.println();
			System.out.println();


			JavaPairRDD<String,Float> RatingInfluence = beforeAfter.mapValues((Tuple2<Float,Long> t1) ->
			(t1._1 / (float) t1._2));
			PrintWriter pwriter = new PrintWriter("AllNormalUserInfluence.json","UTF-8");

			List<Tuple2<String,Float>> answers = RatingInfluence.collect();
			float average = 0;
			int count = 0;
			for(Tuple2<String,Float> answer : answers){
				//average = average + answer._2;
				//count++;
				pwriter.println("{\"normalId\":\"" + answer._1 + "\",\"influence\":\"" + answer._2 + "\"}");
			}
			//pwriter.println("Average = " + average/count);
			
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
