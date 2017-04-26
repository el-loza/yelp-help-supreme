//mvn clean compile package
//"$SPARK_HOME"/bin/spark-submit --class "temp" --master spark://kinshasa:7077 target/temp-0.0.1.jar

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import static org.apache.spark.sql.functions.col;

public class MonthlyInfluencePoints {

	public static SparkSession spark; 

	//Determine if the elite rating matches the restaurant's next month/year rating
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
			
			
			//Populating dataset for user
			Dataset<Row> dsUsers = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_user.json");
			Dataset<Row> users = dsUsers.select(col("user_id"),col("elite"));//smaller dataset with user_id and elite years
			Dataset<Row> notElite = users
					.select(col("user_id").as("user2"),col("elite").as("elite2"))
					.where(functions.array_contains(col("elite"), "None"));//dataset with all users that are not elite (used for join)
					
					//Remove this line if you want to run it on elite users and not normal users.
					//.limit(47343);//smaller dataset with user_id and elite years
			
			//Uncomment this portion if you want to run it for elite users
			Dataset<Row> eliteUsers = users
					.select(col("user_id"),col("elite"))
					.join(notElite,col("user2").equalTo(col("user_id")),"left_outer")
					.where(col("user2").isNull())
					.drop(col("user2"))
					.drop(col("elite2"));//dataset that now contains all users that are elite
			eliteUsers.show(10);
			System.out.println("Elite Users!");
			
			
			//Populating dataset for review
			Dataset<Row> dsReview = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_review.json");

			Dataset<Row> reviews = dsReview
					.select(col("review_id"), col("user_id").as("usr_rev_id"), col("business_id"),col("stars"), functions.year(col("date")).as("year"), functions.month(col("date")).as("month"))
					.where(col("user_id").isNotNull());

			reviews.show(10);
			System.out.println("All reviews!!!!");


			Dataset<Row> eliteReviews = reviews
					.select(col("usr_rev_id"), col("business_id"),col("stars"), col("year"), col("month") )
					.join(eliteUsers,col("user_id").equalTo(col("usr_rev_id")));
					
					//Comment this line and uncomment line above if you want to run on elite users
					//.join(users,col("user_id").equalTo(col("usr_rev_id")));
			
			eliteReviews.show(10);
			System.out.println("Elite user reviews! :D >_>");
			Dataset<Row> restaurantYearRating = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/RestaurantPerMonthlyYearRating.json");
			restaurantYearRating.show(10);
			System.out.println("first join");
			
			Dataset<Row> eliteReviewBefore = eliteReviews
					.join(restaurantYearRating, col("business_id").equalTo(col("bid"))
							.and(
								(
									(col("year").$times(12))
										.$plus(col("month"))
								).equalTo(col("monthYear").$minus(1))),"left_outer");
			eliteReviewBefore.show(10);
			
			System.out.println("before renames");
			Dataset<Row> eliteReviewBeforeRename = eliteReviewBefore
					.select(col("user_id"), col("business_id"), col("stars"), col("year"), col("month"), col("rating").as("beforerating"));
			eliteReviewBeforeRename.show(10);
			

			Dataset<Row> eliteReviewBeforeAfter = eliteReviewBeforeRename
					.join(restaurantYearRating, col("business_id").equalTo(col("bid")).and(
								(
									(col("year").$times(12))
										.$plus(col("month"))
								).equalTo(col("monthYear").$plus(1))),"left_outer");
			eliteReviewBeforeAfter.show(10);
			System.out.println("bar renamed");
			Dataset<Row> eliteReviewBARename = eliteReviewBeforeAfter
					.select(col("user_id"), col("business_id"), col("stars"), col("beforerating"), col("rating").as("afterrating"))
					.where(col("afterrating").isNotNull().and(col("beforerating").isNotNull()));
			eliteReviewBARename.show(100);
			

			//Map/Reduce Review
			JavaRDD<Row> eliteReviewsBA = eliteReviewBARename.toJavaRDD();
			JavaPairRDD<String, Tuple2<Float,Long>> beforeAfter = eliteReviewsBA.mapToPair((Row row) -> {
				String key = row.get(0).toString();
				//rating - rating for this review. get(2)
				//beforeRating - rating for the restaurant the year before elite user rating - get(4) 
				//afterRating - rating for the restaurant the year after elite user rating - get(5)
				float equalBuffer = (float) 0.2;
		
				float before = Float.parseFloat(row.get(3).toString());
				float after = Float.parseFloat(row.get(4).toString());
				float rating = Float.parseFloat(row.get(2).toString());

				float weight = (float) 0.5;
				float influence = (weight * getTrend(rating, before, after, equalBuffer)) + ((1 - weight) * getScaling(rating, after));
				long count = 1;

				return new Tuple2<>(key, new Tuple2<>(influence,count));
			}).reduceByKey((Tuple2<Float,Long> t1 , Tuple2<Float,Long> t2)->
			new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));

			JavaPairRDD<String,Float> RatingInfluence = beforeAfter.mapValues((Tuple2<Float,Long> t1) ->
			(t1._1 / (float) t1._2));
			PrintWriter pwriter = new PrintWriter("EliteUserMonthlyInfluence.json","UTF-8");
			float average = 0;
			int count = 0;
			List<Tuple2<String,Float>> answers = RatingInfluence.collect();
			for(Tuple2<String,Float> answer : answers){
				average = average + answer._2;
				count++;
				pwriter.println("{\"eliteID\":\"" + answer._1 + "\",\"influence\":\"" + answer._2 + "\"}");
				//pwriter.println("{\"normalId\":\"" + answer._1 + "\",\"influence\":\"" + answer._2 + "\"}");

			}
			System.out.println("Average = " + average/count);
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