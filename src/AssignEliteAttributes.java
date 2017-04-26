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

import static org.apache.spark.sql.functions.col;

public class AssignEliteAttributes{


	public static SparkSession spark; 
	
	//Break influence into brackets 
	/*
	 * 0: 0 =< x < .1 - Not Influential
	 * 1: .1 =< x < .2 - Barely Influential
	 * 2: .2 =< x < .3 - Somewhat Influential
	 * 3: .3 =< x < .4 - Influential
	 * 4: .4 =< x <= 1 - Highly Influential
	 */
	
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
		//Binary test...
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


			//Populating dataset for user
			Dataset<Row> userList = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/yelp_academic_dataset_user.json");
			//Dataset<Row> userList = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp_academic_dataset_user.json");

			//CHANGE CLUSTER/FILE THAT YOU WANT TO READ
			//Dataset<Row> eliteInfluenceList = spark.read().json("hdfs://salem.cs.colostate.edu:42201/yelp/EliteUserInfluence.json");
			Dataset<Row> eliteInfluenceList = spark.read().json("hdfs://des-moines.cs.colostate.edu:42850/project/yelp/EliteUserMonthlyInfluence.json");

			System.out.println("join tables");
			Dataset<Row> eliteAttributes = userList
					//UNCOMMENT TO RUN AS ELITE USER
					.join(eliteInfluenceList, col("user_id").equalTo(col("eliteID")),"left_outer")
					.where(col("eliteID").isNotNull());

					//UNCOMMENT TO RUN AS NORMAL USER
					//.join(eliteInfluenceList, col("user_id").equalTo(col("normalId")),"left_outer")
					//.where(col("normalId").isNotNull());
			eliteAttributes.show(10);

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
			 * 24 eliteID
			 * 23 influence				yes
			 *
			 */

			JavaRDD<Row> eliteAttributeRDD = eliteAttributes.toJavaRDD();
			JavaPairRDD<String, String> eliteAttributeMap = eliteAttributeRDD.mapToPair((Row row) -> {
				String key = "";
				String output = "";

				//UNCOMMENT TO RUN ELITE USER 
				double influence = Double.parseDouble(row.get(24).toString());
				//UNCOMMENT TO RUN NORMAL USER
				//double influence = Double.parseDouble(row.get(23).toString());
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
						//UNCOMMENT TO USE ALL ATTRIBUTES -- (19 atts)
						//output += " " + (counter) + ":" + list.size();
						//counter++;
					}
					//else if(j == 17 || j == 19 || ( (j <= 11) && (j > 0) ) ) //Removing compliments to user.
					else if(j == 17 || j == 19 ) //Don't add name or type
					{

					}
					else 
					{
						double value = Double.parseDouble(row.get(j).toString());
						//Small set including Cool, Funny, Useful divided by user's total number of reviews
						if(j == 12 || j == 16 || j == 20)
						{
							value = value / Double.parseDouble(row.get(18).toString());
							//UNCOMMENT TO USE ALL ATTRIBUTES
							output += " " + (counter) + ":" + value;
							counter++;
						}
						//UNCOMMENT TO USE ALL ATTRIBUTES -- (19 atts)
						//output += " " + (counter) + ":" + value;
						//counter++;
					}
				}

				//Yelping Since
				//Determined this attribute provided no benefit in fitting data to the Machine Learning model
				//				String value = row.get(22).toString();
				//				String date = value.substring(0,4);
				//				output += " " + counter + ":" + date;

				return new Tuple2<>(key, output);
			});

			//CHANGE NAME-- OUTPUT FILE
			PrintWriter pwriter = new PrintWriter("Elite-MonthlyAttributesQuad.txt","UTF-8");
			List<Tuple2<String,String>> answers = eliteAttributeMap.collect();

			for(Tuple2<String,String> answer : answers){
				//Output libsvm formatted file for Machine learning
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
