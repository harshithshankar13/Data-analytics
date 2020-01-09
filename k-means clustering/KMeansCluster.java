/*
 * Question_1
 * Author : Harshith Shankar Tarikere Ravikumar (19230323)
 * */
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import scala.Tuple2;

public class KMeansCluster {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:/winutils");
		// setting Spark Configuration with Application Name and number of Cores Spark
		// should use.
		SparkConf sparkConf = new SparkConf().setAppName("KMeanCluster").setMaster("local[4]")
				.set("spark.executor.memory", "1g");
		// declaring Java Spark Context using spark Configuration.
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		// Load and parse data
		String path = "twitter2D.txt";
		// JavaRDD of string is created and stored with each and every lines in
		// twitter2D.txt file
		JavaRDD<String> lines = ctx.textFile(path, 1);

		// Creating JavaPairRDD with String[] and Vector in it.
		JavaPairRDD<String[], Vector> parsedData = lines.mapToPair(s -> {
			// splitting each line on comma(,)
			String[] observation = s.split(",");
			// Taking first two index elements from observation which has coordinates.
			double[] values = new double[2];
			for (int i = 0; i < 2; i++) {
				values[i] = Double.parseDouble(observation[i]);
			}
			// returns whole observation and vector with coordinates along with Tuple2
			return new Tuple2<>(observation, Vectors.dense(values));
		});
		parsedData.cache();

		// getting a vector from parsedData in order to train the KMean clustering
		// model.
		JavaRDD<Vector> vectorData = parsedData.map(data -> {
			return data._2();
		});

		int numClusters = 4;
		int numIterations = 20;
		// Cluster the data into four classes and with 20 iterations using KMeans
		KMeansModel kmeanclusters = KMeans.train(vectorData.rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		for (Vector kmeancenter : kmeanclusters.clusterCenters()) {
			System.out.println(kmeancenter);
		}

		// computing cost function of model
		double cost = kmeanclusters.computeCost(vectorData.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = kmeanclusters.computeCost(vectorData.rdd());
		System.out.println("Within Set Sum of Squared Errors : " + WSSSE);

		// Here mapping parsedData's String and vector, and predicting cluster index
		// using kmeanclusters.predict with vector as input.
		// sortByKey is used to sort tuple2 by its key(here predicted cluster index is
		// the key).
		// take() is used to get all the elements in the parsedData.
		// Displaying sorted tuple2 using forEach().
		parsedData.mapToPair(x -> {
			String tweet = "";
			// checking if the last but one (n-2)th element in also belongs to tweet
			// if yes then we are adding (n-2)th element to that tweet.
			if (StringUtils.isNumeric(x._1()[x._1().length - 2]) == false) {
				tweet = x._1()[x._1().length - 2]; // adding tweet data from parsedData to variable tweet.
				tweet += ","; // concatenating comma to tweet
			}

			// adding last element of parsedData, which is a tweet to variable tweet.
			tweet += x._1()[x._1().length - 1];

			// returning predicted label and tweet
			return new Tuple2<Integer, String>(kmeanclusters.predict(x._2), tweet);
		}).sortByKey().take((int) parsedData.count())
				.forEach(t -> System.out.println("Tweet : " + t._2 + " is in cluster " + t._1));

		// stopping JavaSparkContext
		ctx.stop();
		// closing JavaSparkContext
		ctx.close();
	}
}
