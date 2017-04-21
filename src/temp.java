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


public class temp {

	public static SparkSession spark; 
	public static void eliteUserReader(Row eliteReviewRow)
	{
		System.out.println("EliteReviewList Loop:");
		String reviewId = eliteReviewRow.get(0).toString();
		String userId = eliteReviewRow.get(1).toString();
		String businessId = eliteReviewRow.get(2).toString();
		String stars = eliteReviewRow.get(3).toString();
		String date = eliteReviewRow.get(4).toString();
		
		//Calculating before star rating
		double totalStarsBefore = 0;
		Dataset<Row> beforeReviews = spark.sql("SELECT stars FROM eliteReviewsTable " +
				" WHERE date < "+ date + 
				" AND business_id = "+  businessId +
				" ORDER BY date DESC LIMIT 10");
		JavaRDD<Row> beforeReviewsRDD = beforeReviews.toJavaRDD();
		List<Row> beforeList = beforeReviewsRDD.collect();
		for(int j = 0; j < beforeList.size(); j++)
		{
			System.out.println("beforeList Loop:" + j);
			String starBefore = beforeList.get(j).get(0).toString();
			//System.out.println("beforeList Loop2:" + j);
			totalStarsBefore += Double.parseDouble(starBefore);
		}
		System.out.println("UserID: " + userId + " Stars Before: " + totalStarsBefore);
	}
	public static void main(String[] args) {
		try {
			PrintWriter pwriter = new PrintWriter("output.txt","UTF-8");

		//SparkConf conf = new SparkConf().setAppName("testSpark");
		//SparkConf conf = new SparkConf().setAppName("testSpark").setMaster("");
		//JavaSparkContext jsc = new JavaSparkContext(conf);
		//JavaRDD<String> yelpDataset = jsc.textFile("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");
		//SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
		
		spark = SparkSession
				.builder()
				.appName("testSpark")
				.enableHiveSupport()	
				.getOrCreate();
		spark.conf().set("spark.executor.memory", "4g");
//******Populating dataset for user**************************************************************************************************************
		Dataset<Row> dsUsers = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");

		Dataset<Row> users = dsUsers.select(col("user_id"),col("elite"));//smaller dataset with user_id and elite years
		//users.persist(StorageLevel.MEMORY_AND_DISK());
		Dataset<Row> notElite = users
				.select(col("user_id").as("user2"),col("elite").as("elite2"))
				.where(functions.array_contains(col("elite"), "None"));//dataset with all users that are not elite (used for join)
		
		Dataset<Row> eliteUsers = users
				.select(col("user_id"),col("elite"))
				.join(notElite,col("user2").equalTo(col("user_id")),"left_outer")
				.where(col("user2").isNull())
				.orderBy(col("user_id").asc())
				.drop(col("user2"))
				.drop(col("elite2"));//dataset that now contains all users that are elite
		eliteUsers.show(10);
		System.out.println("Elite Users!");
	
//******Populating dataset for review**************************************************************************************************************
		Dataset<Row> dsReview = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_review.json");
		
		Dataset<Row> reviews = dsReview
				.select(col("review_id"), col("user_id").as("usr_rev_id"), col("business_id"),col("stars"), col("date"), functions.year(col("date")).as("year"))
				.where(col("user_id").isNotNull())
				.orderBy(col("business_id").asc(), col("date").asc());
		reviews.persist(StorageLevel.MEMORY_AND_DISK());
		reviews.show(10);
		System.out.println("All reviews!!!!");
		
		Dataset<Row> eliteReviews = reviews
				.select(col("review_id"), col("usr_rev_id"), col("business_id"),col("stars"), col("date"))
				.join(eliteUsers,col("user_id").equalTo(col("usr_rev_id")))
				//.where(functions.array_contains(col("elite"), col("year")))
				//.where(col("user_id").isNull())
				.orderBy(col("usr_rev_id").asc())
				.orderBy(col("date").asc());
		eliteReviews.show(10);
		System.out.println("Elite user reviews! :D >_>");	
		
		JavaRDD<Row> eliteReviewsRDD = eliteReviews.toJavaRDD();
		
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
			 
			 
			 
			 */
			pwriter.println("UserID: " + userId + " Stars Before: " + totalStarsBefore);
			
		}
        
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
