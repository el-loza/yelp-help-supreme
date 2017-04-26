
// $example on$
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
// $example off$
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
	
/**
 * An example for Multilayer Perceptron Classification.
 */
public class testML {

	public static void main(String[] args) {



		SparkSession spark = SparkSession
				.builder()
				.appName("JavaMultilayerPerceptronClassifierExample")
				.getOrCreate();

		PrintWriter pw;
		try {
			pw = new PrintWriter("./EliteMonthly-500HiddenIterInc.txt", "UTF-8");


			// $example on$
			// Load training data
			//String path = "hdfs://salem.cs.colostate.edu:42201/yelp/EliteAttributes.txt";
			//String path = "hdfs://salem.cs.colostate.edu:42201/yelp/quad-EliteAttributes.txt";
			//String path = "hdfs://salem.cs.colostate.edu:42201/yelp/monthly-EliteAttributes.txt";
			//String path = "hdfs://des-moines.cs.colostate.edu:42850/project/yelp/Elite-MonthlyAttributesQuad.txt";
			String path = "hdfs://des-moines.cs.colostate.edu:42850/project/yelp/Elite-MonthlyAttributes.txt";
			Dataset<Row> dataFrame = spark.read().format("libsvm").load(path);


			// Split the data into train and test
			Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
			Dataset<Row> train = splits[0];
			Dataset<Row> test = splits[1];

			// specify layers for the neural network:
			// input layer of size 4 (features), two intermediate of size 5 and 4
			// and output of size 3 (classes)
			//int[] layers = new int[] {20, 5, 2, 4, 5};
			ArrayList<int[]> diffLayers = new ArrayList();
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			diffLayers.add(new int[] {19, 500, 5});
			//diffLayers.add(new int[] {3, 700, 5});
			//diffLayers.add(new int[] {3, 500, 100, 5});
			//diffLayers.add(new int[] {19, 750, 5});
			//diffLayers.add(new int[] {19, 1000, 5});
			//diffLayers.add(new int[] {19, 1200, 5});
			//diffLayers.add(new int[] {19, 50, 8});
			//diffLayers.add(new int[] {19, 100, 8});
			//diffLayers.add(new int[] {19, 500, 8});
			//diffLayers.add(new int[] {19, 10, 20, 30, 8});
			//diffLayers.add(new int[] {19, 30, 30, 30, 8});
			//diffLayers.add(new int[] {19, 30, 20, 5, 8});
			//diffLayers.add(new int[] {19, 50, 40, 30, 20, 10, 5, 2, 8});
			//diffLayers.add(new int[] {19, 20, 20, 20, 20, 20, 20, 20, 20, 8});
			//diffLayers.add(new int[] {19, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 5});
			//diffLayers.add(new int[] {19, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 20, 5});
			//diffLayers.add(new int[] {19, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 50, 5});

			for (int i =0; i < diffLayers.size(); i++){


				// create the trainer and set its parameters
				MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()
						.setLayers(diffLayers.get(i))
						.setBlockSize(128)
						.setSeed(1234L)
						.setMaxIter(50 + (50 * i));

				// train the model
				MultilayerPerceptronClassificationModel model = trainer.fit(train);

				// compute accuracy on the test set
				Dataset<Row> result = model.transform(test);
				Dataset<Row> predictionAndLabels = result.select("prediction", "label");

				MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
						.setMetricName("accuracy");
				
				MulticlassMetrics metrics = new MulticlassMetrics(predictionAndLabels);

				pw.println("Neural Network (Layer Config: " + i + " Iter=1000) Test set accuracy = " + evaluator.evaluate(predictionAndLabels));
				pw.println("Confusion Matrics: \n" + metrics.confusionMatrix());
				pw.println();
				pw.println("False Positive Matrics: \n" + metrics.weightedFalsePositiveRate());
				pw.println();
				pw.println("True Positive Matrics: \n" + metrics.weightedTruePositiveRate());
				// $example off$
			}

			//int[] nIterations = {100, 200, 500, 1000, 2000};
			int[] nIterations = {100};

			
			for (int j = 0; j < nIterations.length; j++){

				// configure the base classifier.
				LogisticRegression classifier = new LogisticRegression()
						.setMaxIter(nIterations[j])
						.setTol(1E-6)
						.setFitIntercept(true);

				// instantiate the One Vs Rest Classifier.
				OneVsRest ovr = new OneVsRest().setClassifier(classifier);

				// train the multiclass model.
				OneVsRestModel ovrModel = ovr.fit(train);

				// score the model on test data.
				Dataset<Row> predictions = ovrModel.transform(test)
						.select("prediction", "label");

				MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
						.setMetricName("accuracy");

				// compute the classification error on test data.
				double accuracy = evaluator.evaluate(predictions);
				pw.println("One V. Rest Model Iter = " + j + " Test Error : " + (1 - accuracy));
				MulticlassMetrics metrics = new MulticlassMetrics(predictions);
				pw.println("Confusion Matrics: \n" + metrics.confusionMatrix());
				pw.println();
				pw.println("False Positive Matrics: \n" + metrics.weightedFalsePositiveRate());
				pw.println();
				pw.println("True Positive Matrics: \n" + metrics.weightedTruePositiveRate());
			}
			
			pw.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	}
}
