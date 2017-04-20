
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

import java.util.List;


public class YelpHelpSupreme {
	
	public static void main (String[] args) {
		//Slide 8 HelpSession 11
		SparkConf sparkConf = new SparkConf().setAppName("YelpHelpSupreme");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);
		Configuration hadoopConfiguration = jsc.hadoopConfiguration();

		//hdfs is pointing to kalvin's cluster
		JavaRDD<String> yelpDataset = jsc.textFile("hdfs://des-moines:42850/project/yelp/yelp_academic_dataset_checkin.json");
		JavaRDD<String> filterYelp = yelpDataset.filter((String line) -> {
			String sentence = line;
			return true;
		}).cache();


		//Slide 9 HelpSession 11
		//Tuple2<Long, Long> is from scala
		JavaPairRDD<String, Long> combinedRDD = filterYelp.mapToPair((String line) -> {
			//				String state = line;
			//				long owned = Long.parseLong(line.substring(1803, 1812));
			//				long rented = Long.parseLong(line.substring(1812, 1821));

			String hi = "hi";
			//return hi;
			long value = 1;
			return new Tuple2<>(hi,value);
			//return new Tuple2<>(state, new Tuple2<>(owned, rented));
		}).reduceByKey((Long t1, Long t2) -> (t1 + t2));

		//Slide 10 HelpSession 11
		JavaPairRDD<String, Long> answerRDD = combinedRDD.mapValues((Long t1) -> t1);

		List<Tuple2<String, Long>> answer = answerRDD.collect();

		for (Tuple2<String, Long> answer1 : answer) {
			System.out.println("are we gods?? god: " + answer1._1 + " ---- total lines: " + answer1._2);
		}

	}

}
