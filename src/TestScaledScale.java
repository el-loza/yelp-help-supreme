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

public class TestScaledScale{


	public static SparkSession spark; 
	
	public static int influenceMapper(double influence)
	{
		if(influence < .1)
		{
			return 0;
		}
		else if(influence < .2)
		{
			return 1;
		}
		else if(influence < .3)
		{
			return 2;
		}
		else if(influence < .4)
		{
			return 3;
		}
		else if(influence <= 1)
		{
			return 4;
		}
		else
		{
			System.out.println("Influence input error!");
			return -1;
		}
//		if (influence < .5) {
//			return 0;
//		} else {
//			return 1;
//		}
	}
	public static void main(String[] args) {
		try {

			spark = SparkSession
					.builder()
					.appName("testSpark")
					.enableHiveSupport()	
					.getOrCreate();
			
			
			//******Populating dataset for user**************************************************************************************************************
			Dataset<Row> userList = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_user.json");

			Dataset<Row> eliteInfluenceList = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/EliteUserInfluence.json");

			System.out.println("join tables");
			Dataset<Row> eliteAttributes = userList
					.join(eliteInfluenceList, col("user_id").equalTo(col("eliteID")),"left_outer")
					.where(col("eliteID").isNotNull());
			eliteAttributes.show(10);
			
			//Break influence into brackets 
			/*
			 * 1: 0 =< x < .2 - Not Influential
			 * 2: .2 =< x < .4 - Barely Influential
			 * 3: .4 =< x < .6 - Somewhat Influential
			 * 4: .6 =< x < .8 - Influential
			 * 5: .8 =< x =< 1 - Highly Influential
			 */
			
			/*
			 * 0 average_stars----------yes
			 * 1 compliment_cool 		yes
			 * 2 compliment_cute 		yes
			 * 3 compliment_funny 		yes
			 * 4 compliment_hot 		yes
			 * 5 compliment_list 		yes
			 * 6 compliment_more		yes
			 * 7 compliment_note		yes
			 * 8 compliment_photos		yes
			 * 9 compliment_plain		yes
			 * 10 compliment_profile	yes
			 * 11 compliment_writer		yes
			 * 12 cool					yes
			 * 13 elite - years			yes
			 * 14 fans					yes
			 * 15 friends - years		yes
			 * 16 funny	----------------yes
			 * 17 name
			 * 18 review_count----------yes
			 * 19 type
			 * 20 useful----------------yes
			 * 21 user_id
			 * 22 yelping_since---------yes
			 * 23 eliteID
			 * 24 influence				yes
			 *
			 */
			
			JavaRDD<Row> eliteAttributeRDD = eliteAttributes.toJavaRDD();
			JavaPairRDD<String, String> eliteAttributeMap = eliteAttributeRDD.mapToPair((Row row) -> {
				String key = "";
				String output = "";
				double influence = Double.parseDouble(row.get(24).toString());
				int influenceScaled = influenceMapper(influence);
				if(influenceScaled == -1)
				{
					return null;
				}
				output += influenceScaled;
				int counter = 1;
				for(int j = 0; j < 21; j++)
				{
					if(j == 13 || j == 15) //Elite list or Friends list - count number of elements in array.
					{
						List<String> list = row.getList(j);
						output += " " + (counter) + ":" + list.size();
						counter++;
					}
//					else if(j == 17 || j == 19 || ( (j <= 11) && (j > 0) ) ) //Don't add name or type
					else if(j == 17 || j == 19 ) //Don't add name or type
					{
						
					}
					else 
					{
						double value = Double.parseDouble(row.get(j).toString());
						if(j == 12 || j == 16 || j == 20 || j == 1 || j == 2 || j == 3 || j == 4 || j == 5 || j == 6 || j == 7 || j == 8 || j == 9 || j == 10 || j == 11)
						{
							value = value / Double.parseDouble(row.get(18).toString());
//							output += " " + (counter) + ":" + value;
//							counter++;
						}
						output += " " + (counter) + ":" + value;
						counter++;
					}
				}
				
				//Yelping Since
//				String value = row.get(22).toString();
//				String date = value.substring(0,4);
//				output += " " + counter + ":" + date;
				
				return new Tuple2<>(key, output);
			});
			
			
			PrintWriter pwriter = new PrintWriter("scaledattributescale-EliteAttributes.txt","UTF-8");
			List<Tuple2<String,String>> answers = eliteAttributeMap.collect();
			
			for(Tuple2<String,String> answer : answers){

				pwriter.println(answer._2);

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
