import java.util.List;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import static org.apache.spark.sql.functions.col;
public class temp {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("testSpark").setMaster("local[2]");
		//SparkConf conf = new SparkConf().setAppName("testSpark").setMaster("");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		//JavaRDD<String> yelpDataset = jsc.textFile("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
		DataFrame df = sqlContext.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/yelp_academic_dataset_user.json");
		df.printSchema();
		DataFrame filteredDF = df.select(col("user_id"),col("elite"));
		JavaRDD<Row> users = filteredDF.toJavaRDD();
        List<Row> exil = users.take(10);
        for(int i = 0; i < 10; i++){
			System.out.println(exil.get(i).get(0));
			List<Row> bana = exil.get(i).getList(1);
			for(int j = 0; j < bana.size();j++){
				System.out.println(bana.get(j));
			}
		}
		
		/*
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
	jsc.close();
	}
}
