import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;

/**
 * An example for Multilayer Perceptron Classification.
 */
public class testRegressionML {

	public static void main(String[] args) {



		SparkSession spark = SparkSession
				.builder()
				.appName("JavaMultilayerPerceptronClassifierExample")
				.getOrCreate();

		PrintWriter pw;
		try {
			pw = new PrintWriter("./RegressionMonthlyUserInfluence.txt", "UTF-8");


			// $example on$
			// Load training data
			//String path = "hdfs://salem.cs.colostate.edu:42201/yelp/EliteAttributes.txt";
			//String path = "hdfs://salem.cs.colostate.edu:42201/yelp/quad-EliteAttributes.txt";
			//String path = "hdfs://salem.cs.colostate.edu:42201/yelp/monthly-EliteAttributes.txt";
			//String path = "hdfs://des-moines.cs.colostate.edu:42850/project/yelp/scaledExtremeScaledAtts-EliteAttributes.txt";
			String path = "hdfs://charleston.cs.colostate.edu:41479/cs455/yelp/EliteAttributesRawInfluence.txt";
			Dataset<Row> dataFrame = spark.read().format("libsvm").load(path);

			// Automatically identify categorical features, and index them.
			// Set maxCategories so features with > 4 distinct values are treated as continuous.
			VectorIndexerModel featureIndexer = new VectorIndexer()
					.setInputCol("features")
					.setOutputCol("indexedFeatures")
					.setMaxCategories(4)
					.fit(dataFrame);


			// Split the data into train and test
			Dataset<Row>[] splits = dataFrame.randomSplit(new double[]{0.6, 0.4}, 1234L);
			Dataset<Row> train = splits[0];
			Dataset<Row> test = splits[1];

			// Train a DecisionTree model.
			DecisionTreeRegressor dt = new DecisionTreeRegressor()
					.setFeaturesCol("indexedFeatures");

			// Chain indexer and tree in a Pipeline.
			Pipeline pipeline = new Pipeline()
					.setStages(new PipelineStage[]{featureIndexer, dt});

			// Train model. This also runs the indexer.
			PipelineModel model = pipeline.fit(train);

			// Make predictions.
			Dataset<Row> predictions = model.transform(test);

			// Select example rows to display.
			predictions.select("*").show(100);

			// Select (prediction, true label) and compute test error.
			RegressionEvaluator evaluator = new RegressionEvaluator()
					.setLabelCol("label")
					.setPredictionCol("prediction")
					.setMetricName("rmse");
			//RegressionMetrics metrics = new RegressionMetrics(predictions);
			double rmse = evaluator.evaluate(predictions);
			pw.println("Decision Tree Root Mean Squared Error (RMSE) on test data = " + rmse);

			//-----------------------------------------------------------------------------

			DecisionTreeRegressionModel treeModel =
					(DecisionTreeRegressionModel) (model.stages()[1]);
			pw.println("Learned regression tree model:\n" + treeModel.toDebugString());

			// Train a RandomForest model.
			RandomForestRegressor rf = new RandomForestRegressor()
					.setLabelCol("label")
					.setFeaturesCol("indexedFeatures");

			// Chain indexer and forest in a Pipeline
			Pipeline pipeline2 = new Pipeline()
					.setStages(new PipelineStage[] {featureIndexer, rf});

			// Train model. This also runs the indexer.
			PipelineModel model2 = pipeline2.fit(train);

			// Make predictions.
			Dataset<Row> predictions2 = model2.transform(test);

			// Select example rows to display.
			predictions2.select("*").show(100);;

			// Select (prediction, true label) and compute test error
			RegressionEvaluator evaluator2 = new RegressionEvaluator()
					.setLabelCol("label")
					.setPredictionCol("prediction")
					.setMetricName("rmse");
			double rmse2 = evaluator2.evaluate(predictions2);
			//pw.println("Root Mean Squared Error (RMSE) on test data = " + rmse2);

			RandomForestRegressionModel rfModel = (RandomForestRegressionModel)(model2.stages()[1]);
			pw.println("Forest Model Learned regression forest model:\n" + rfModel.toDebugString());

			//-----------------------------------------------------------------------------------------

			// Train a GBT model.
			GBTRegressor gbt = new GBTRegressor()
					.setLabelCol("label")
					.setFeaturesCol("indexedFeatures")
					.setMaxIter(10);

			// Chain indexer and GBT in a Pipeline.
			Pipeline pipeline3 = new Pipeline().setStages(new PipelineStage[] {featureIndexer, gbt});

			// Train model. This also runs the indexer.
			PipelineModel model3 = pipeline3.fit(train);

			// Make predictions.
			Dataset<Row> predictions3= model3.transform(test);

			// Select example rows to display.
			predictions3.select("*").show(100);;

			// Select (prediction, true label) and compute test error.
			RegressionEvaluator evaluator3 = new RegressionEvaluator()
					.setLabelCol("label")
					.setPredictionCol("prediction")
					.setMetricName("rmse");
			double rmse3 = evaluator3.evaluate(predictions3);
			//pw.println("Root Mean Squared Error (RMSE) on test data = " + rmse3);

			GBTRegressionModel gbtModel = (GBTRegressionModel)(model3.stages()[1]);
			pw.println("Learned regression GBT model:\n" + gbtModel.toDebugString());

			//----------------------------------------------------------------------------
			LinearRegression lr = new LinearRegression()
					.setMaxIter(10)
					.setRegParam(0.3)
					.setElasticNetParam(0.8);

			// Fit the model.
			LinearRegressionModel lrModel = lr.fit(train);


			// Print the coefficients and intercept for linear regression.
			System.out.println("Coefficients: "
					+ lrModel.coefficients() + " Intercept: " + lrModel.intercept());

			// Summarize the model over the training set and print out some metrics.
			LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
			pw.println("Linear Regression numIterations: " + trainingSummary.totalIterations());
			pw.println("Lienar Regressino objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
			trainingSummary.residuals().show();
			pw.println("Linear Regression RMSE: " + trainingSummary.rootMeanSquaredError());
			pw.println("Linear Regression r2: " + trainingSummary.r2());

			// Make predictions.
			Dataset<Row> predictions4= lrModel.transform(test);
			// Select example rows to display.
			predictions4.select("prediction", "label", "features").show(5);
			// Select (prediction, true label) and compute test error.
			RegressionEvaluator evaluator4 = new RegressionEvaluator()
					.setLabelCol("label")
					.setPredictionCol("prediction")
					.setMetricName("rmse");
			double rmse4 = evaluator4.evaluate(predictions4);
			pw.println("Linear Regression Root Mean Squared Error (RMSE) on test data = " + rmse4);


			pw.close();
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		spark.stop();
	}
}