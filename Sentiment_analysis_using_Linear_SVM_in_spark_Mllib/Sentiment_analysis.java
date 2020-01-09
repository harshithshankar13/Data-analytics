import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;


import scala.Serializable;
import scala.Tuple2;

public class Sentiment_analysis {
	
	public static void main(String[] args) {
	System.setProperty("hadoop.home.dir", "C:/winutils");
	SparkConf sparkConf = new SparkConf().setAppName("countTemperature").setMaster("local[4]")
			.set("spark.executor.memory", "1g");
	JavaSparkContext ctx = new JavaSparkContext(sparkConf);
	
	JavaRDD<String> lines = ctx.textFile("imdb_labelled.txt", 1);

	final HashingTF tf = new HashingTF(100);
	
	JavaRDD<LabeledPoint> data = lines.map(l -> {
		
		 String[] lineSplit = l.split("  	");
		 
		 String comment = lineSplit[0];
		 double label = Double.parseDouble(lineSplit[1].trim());
		 
//		 LabeledPoint labeleldPoint = 
		Vector vec = tf.transform(Arrays.asList(comment.split(" ")));
		LabeledPoint labe =  new LabeledPoint(label , vec);	 
		return labe;
	});
	
	// Split initial RDD into two... [60% training data, 40% testing data].
	JavaRDD<LabeledPoint> training = data.sample(false, 0.6, 13L);
	training.cache();
	
	JavaRDD<LabeledPoint> test = data.subtract(training);
	

	// Run training algorithm to build the model.
	int numIterations = 100;
	SVMModel model = SVMWithSGD.train(training.rdd(), numIterations);
	
	// Clear the default threshold.
	model.clearThreshold();

	// Compute raw scores on the test set.
	JavaRDD<Tuple2<Object, Object>> scoreAndLabels = test.map(p ->
	  new Tuple2<>(model.predict(p.features()), p.label()));
	
	// Get evaluation metrics.
	BinaryClassificationMetrics metrics =
	  new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
	double auROC = metrics.areaUnderROC();

	System.out.println("Area under ROC = " + auROC);
	
	}
}
