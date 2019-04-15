package CollaborativeFiltering;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RecommendationModel {
	


	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf()
				.setJars(new String[] { "E:\\论文实验\\mjsl1.jar" })
				.set("spark.num.executors", "4")
				.set("spark.executor.cores", "3")
				.set("spark.default.parallelism", "12")
				.set("spark.executor.memory", "2048m")
				.set("spark.network.timeout", "300")
			/*	.setMaster("local[4]");*/
				.setMaster("spark://192.168.2.151:7077");
		SparkSession spark = SparkSession.builder().appName("RecommendationModel").config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		JavaRDD<Rating> ratingsRDD = spark
				  .read().textFile("hdfs://192.168.2.151:9000/test/hjw/sample/sample_movielens_ratings.txt").javaRDD()
				  .map(new Function<String, Rating>() {
				    public Rating call(String str) {
				      return Rating.parseRating(str);
				    }
				  });
				Dataset<Row> ratings = spark.createDataFrame(ratingsRDD, Rating.class);
				Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
				Dataset<Row> training = splits[0];
				Dataset<Row> test = splits[1];

				// Build the recommendation model using ALS on the training data
				ALS als = new ALS()
				  .setMaxIter(5)
				  .setRegParam(0.01)
				  .setUserCol("userId")
				  .setItemCol("movieId")
				  .setRatingCol("rating");
				ALSModel model = als.fit(training);

				// Evaluate the model by computing the RMSE on the test data
				Dataset<Row> predictions = model.transform(test);

				
		
				
				RegressionEvaluator evaluator = new RegressionEvaluator()
				  .setMetricName("rmse")
				  .setLabelCol("rating")
				  .setPredictionCol("prediction");
				Double rmse = evaluator.evaluate(predictions);
				System.out.println("Root-mean-square error = " + rmse);
				
				predictions.show();
				
			    // Generate top 10 movie recommendations for each user
			    //Dataset<Row> userRecs = model.recommendForAllUsers(10);
			    // Generate top 10 user recommendations for each movie
			    //Dataset<Row> movieRecs = model.recommendForAllItems(10);

			    // Generate top 10 movie recommendations for a specified set of users
			    Dataset<Row> users = ratings.select(als.getUserCol()).distinct().limit(3);
			    
			   // Dataset<Row> userSubsetRecs = model.recommendForUserSubset(users, 10);
			    // Generate top 10 user recommendations for a specified set of movies
			    Dataset<Row> movies = ratings.select(als.getItemCol()).distinct().limit(3);
			    //Dataset<Row> movieSubSetRecs = model.recommendForItemSubset(movies, 10);
			    // $example off$
			   // userRecs.show();
			    //movieRecs.show();
			   // userSubsetRecs.show();
			   // movieSubSetRecs.show();

			    spark.stop();
	}
	
}
