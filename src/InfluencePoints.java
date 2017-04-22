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


public class InfluencePoints {

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
		Dataset<Row> dsUsers = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");
		Dataset<Row> users = dsUsers.select(col("user_id"),col("elite"));//smaller dataset with user_id and elite years
		Dataset<Row> notElite = users
				.select(col("user_id").as("user2"),col("elite").as("elite2"))
				.where(functions.array_contains(col("elite"), "None"));//dataset with all users that are not elite (used for join)

		Dataset<Row> eliteUsers = users
				.select(col("user_id"),col("elite"))
				.join(notElite,col("user2").equalTo(col("user_id")),"left_outer")
				.where(col("user2").isNull())
				.drop(col("user2"))
				.drop(col("elite2"));//dataset that now contains all users that are elite
		eliteUsers.show(10);
		System.out.println("Elite Users!");
	
//******Populating dataset for review**************************************************************************************************************
		Dataset<Row> dsReview = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_review.json");
		
		Dataset<Row> reviews = dsReview
				.select(col("review_id"), col("user_id").as("usr_rev_id"), col("business_id"),col("stars"), functions.year(col("date")).as("year"))
				.where(col("user_id").isNotNull());

		reviews.show(10);
		System.out.println("All reviews!!!!");
		
		
		Dataset<Row> eliteReviews = reviews
				.select(col("usr_rev_id"), col("business_id"),col("stars"), col("year").as("elite_year"))
				.join(eliteUsers,col("user_id").equalTo(col("usr_rev_id")));
		eliteReviews.show(10);
		System.out.println("Elite user reviews! :D >_>");
			Dataset<Row> restaurantYearRating = spark.read().json("RestaurantPerYearRating.json");
		
		Dataset<Row> eliteReviewBefore = eliteReviews
							.select(col("user_id"), col("business_id"), col("stars"), col("elite_year"), col("rating").as("beforeRating"))
							.join(restaurantYearRating, col("business_id").equalTo(col("bid")).and(col("year").equalTo(col("elite_year").$minus(1))),"left_inner");
		eliteReviewBefore.show(10);
		
		Dataset<Row> eliteReviewBeforeAfter = eliteReviewBefore
				.select(col("user_id"), col("business_id"), col("stars"), col("elite_year"), col("beforeRating"), col("rating").as("afterRating"))
				.join(restaurantYearRating, col("business_id").equalTo(col("bid")).and(col("year").equalTo(col("elite_year").$plus(1))),"left_inner");
		eliteReviewBeforeAfter.show(10);
			
//****** Map/Reduce Review**************************************************************************************************************
		JavaRDD<Row> eliteReviewsBA = eliteReviewBeforeAfter.toJavaRDD();
		JavaPairRDD<String, Tuple2<Float,Long>> beforeAfter = eliteReviewsBA.mapToPair((Row row) -> {
			String key = row.get(0).toString();
			//rating - rating for this review. get(2)
			//beforeRating - rating for the restaurant the year before elite user rating - get(4) 
			//afterRating - rating for the restaurant the year after elite user rating - get(5)
			float equalBuffer = (float) 0.2;
			
			float before = Float.parseFloat(row.get(4).toString());
			float after = Float.parseFloat(row.get(5).toString());
			float rating = Float.parseFloat(row.get(2).toString());
			
			float weight = (float) 0.5;
			float influence = (weight * getTrend(rating, before, after, equalBuffer)) + ((1 - weight) * getScaling(rating, after));
			long count = 1;
			
			return new Tuple2<>(key, new Tuple2<>(influence,count));
		}).reduceByKey((Tuple2<Float,Long> t1 , Tuple2<Float,Long> t2)->
		new Tuple2<>(t1._1 + t2._1, t1._2 + t2._2));
		
		JavaPairRDD<String,Float> RatingInfluence = beforeAfter.mapValues((Tuple2<Float,Long> t1) ->
		(t1._1 / (float) t1._2));

		List<Tuple2<String,Float>> answers = RatingInfluence.collect();
		for(Tuple2<String,Float> answer : answers){
			PrintWriter pwriter = new PrintWriter("EliteUserInfluence.json","UTF-8");
			pwriter.println(answer._1 + ", " + answer._2);
			
			pwriter.close();
		}
		
//****** Map/Reduce Elite Reviews**************************************************************************************************************		
	//	JavaRDD<Row> eliteReviewsRDD = eliteReviews.toJavaRDD();
		
		
		/*
		//userDF.show();
        List<Row> eliteReviewList = eliteReviewsRDD.take(5);//.forEach(eliteUserReader);;
		//List<Row> userYearPair = new ArrayList<Row>();
		eliteReviews.createOrReplaceTempView("eliteReviewsTable");
		
        for(int i = 0; i < eliteReviewList.size(); i++){
        	pwriter.println("EliteReviewList Loop:" + i);
			String reviewId = eliteReviewList.get(i).get(0).toString();
			String userId = eliteReviewList.get(i).get(1).toString();
			String businessId = eliteReviewList.get(i).get(2).toString();
			String stars = eliteReviewList.get(i).get(3).toString();
			String date = eliteReviewList.get(i).get(4).toString();
			
			//Calculating before star rating
			double totalStarsBefore = 0;
			Dataset<Row> beforeReviews = spark.sql("SELECT stars FROM eliteReviewsTable " +
					" WHERE date < '"+ date + 
					"' AND business_id = '"+  businessId +
					"' ORDER BY date DESC LIMIT 10");
			JavaRDD<Row> beforeReviewsRDD = beforeReviews.toJavaRDD();
			List<Row> beforeList = beforeReviewsRDD.collect();
			for(int j = 0; j < beforeList.size(); j++)
			{
				pwriter.println("beforeList Loop:" + j);
				String starBefore = beforeList.get(j).get(0).toString();
				//System.out.println("beforeList Loop2:" + j);
				totalStarsBefore += Double.parseDouble(starBefore);
			}
			/*
			 
			 foreach
			 
			 
			 
			 
			pwriter.println("UserID: " + userId + " Stars Before: " + totalStarsBefore);
			
		}
        */
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        /*
        //JavaRDD<Row> userPair = parallelize(userYearPair);
        //convert userYearPair to dataset. 
        //run query on new dataset
		
		
		JavaRDD<String> filterYelp = users.filter((String line) -> {
			//if the elite [] is empty return false
			//else return true
			return true;
		}).cache();

		JavaPairRDD<String, Long> combinedRDD = filterYelp.mapToPair((String line) -> {
			//Get user_id
			//get Array of Elite years
			String hi = "hi";
			long value = 1;
			return new Tuple2<>(hi,value);
		}).reduceByKey((Long t1, Long t2) -> (t1 + t2));

		//Slide 10 HelpSession 11
		JavaPairRDD<String, Long> answerRDD = combinedRDD.mapValues((Long t1) -> t1);

		List<Tuple2<String, Long>> answer = answerRDD.collect();

		for (Tuple2<String, Long> answer1 : answer) {
			System.out.println("Neo Kalvinnism: " + answer1._1 + " ---- total lines: " + answer1._2);
		}*/
	//jsc.close();
	}
}
