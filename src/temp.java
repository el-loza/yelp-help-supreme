//mvn clean compile package
//"$SPARK_HOME"/bin/spark-submit --class "temp" --master spark://kinshasa:7077 target/temp-0.0.1.jar

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

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

	public static void main(String[] args) {
		//SparkConf conf = new SparkConf().setAppName("testSpark");
		//SparkConf conf = new SparkConf().setAppName("testSpark").setMaster("");
		//JavaSparkContext jsc = new JavaSparkContext(conf);
		//JavaRDD<String> yelpDataset = jsc.textFile("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");
		//SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
		
		SparkSession spark = SparkSession
				.builder()
				.appName("testSpark")
				.enableHiveSupport()	
				.getOrCreate();
		
//******Populating dataset for user**************************************************************************************************************
		Dataset<Row> dsUsers = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");

		Dataset<Row> users = dsUsers.select(col("user_id"),col("elite"));//smaller dataset with user_id and elite years
		users.persist(StorageLevel.MEMORY_AND_DISK());
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
				.select(col("review_id"), col("user_id").as("usr_rev_id"), col("business_id"),col("stars"), col("date"))
				.where(col("user_id").isNotNull())
				.orderBy(col("business_id").asc(), col("date").asc());
		reviews.persist(StorageLevel.MEMORY_AND_DISK());
		reviews.show(10);
		System.out.println("All reviews!!!!");
		
		Dataset<Row> eliteReviews = reviews
				.select(col("review_id"), col("usr_rev_id"), col("business_id"),col("stars"), col("date"))
				.join(eliteUsers,col("user_id").equalTo(col("usr_rev_id")))
				//.where(functions.array_contains(col("elite"), functions.year(col("date")).toString()))
				//.where(col("user_id").isNull())
				.orderBy(col("usr_rev_id").asc())
				.orderBy(col("date").asc());
		eliteReviews.show(10);
		System.out.println("Elite user reviews! :D >_>");
		
		
		
		//JavaRDD<Row> users = userDF.toJavaRDD();
		//userDF.show();
        //List<Row> exil = users.collect();
		//List<Row> userYearPair = new ArrayList<Row>();
		
		/*
        for(int i = 0; i < 10; i++){
			System.out.println(exil.get(i).get(0));
			List<Row> bana = exil.get(i).getList(1);
			for(int j = 0; j < bana.size();j++){
				System.out.println(bana.get(j));
				Row ax = RowFactory.create(exil.get(i).get(0), bana.get(j));
				userYearPair.add(ax);
			}
		}
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
