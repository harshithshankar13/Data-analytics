package tweetStreaming;

import org.apache.spark.streaming.Durations;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

import twitter4j.Status;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * @author 19230323
 *
 */

public class Question_1 {

	public static void main(String[] args) {
		// Set the system properties so that Twitter4j library used by Twitter stream
		// can use them to generate OAuth credentials
		System.setProperty("twitter4j.oauth.consumerKey", "ov66oV3uoK6qzvPD4RSnvoncU");
		System.setProperty("twitter4j.oauth.consumerSecret", "tqj9H0HH7I1q9oGldVghmAKjNnpwlUMHswmM7cvWsfDNU7NSVp");
		System.setProperty("twitter4j.oauth.accessToken", "594328874-XsF3oQ56UCi3hpETGPaYyq0Of714UjqklK8ANLEk");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "eD7Rhs90F0K0hC5rpbM1rNOiHs7HxyNnduwM8E915ySmA");
		
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		// configure the spark with application name, number of core and memory size.
		SparkConf sparkConf = new SparkConf().setAppName("twitterStreaming").setMaster("local[4]")
				.set("spark.executor.memory", "1g");

		// Create java stream context using sparkConf with one second window of
		// streaming
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
//		jssc.checkpoint("H:\\study\\master\\Nuigalway\\Tools And Technique\\code\\Streaming_checkPoint");

		// creating a tweeter stream
		JavaDStream<Status> tweetStream = TwitterUtils.createStream(jssc);

		// Part-A, Question 1 ------
		JavaDStream<String> tweets = tweetStream.map(new Function<Status, String>() {
			public String call(Status status) {
				return status.getText(); // returns the extracted text from the status (i.e. tweetStream)
			}
		});

		// printing tweets each second
		tweets.print();

		// Part-A, Question 2 ------
		// printing tweet with number of character in each tweet
		JavaPairDStream<String, Integer> tweetCharCount = tweetStream
				.mapToPair(new PairFunction<Status, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(Status s) {
						// returns the pairs of (tweet Text, tweet number of character)
						// length is used to get character length of tweet.
						return new Tuple2<>(s.getText(), s.getText().length());
					}
				});

		// printing tweet along with it's number of character
		tweetCharCount.print();

		// Part A - Question 2 --
		// printing tweet with number of words in tweet
		JavaPairDStream<String, Integer> tweetWordCount = tweetStream
				.mapToPair(new PairFunction<Status, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(Status s) {
						// returns the pairs of (tweet Text, tweet number of words)
						// split tweet text on single space and take length as number of words.
						return new Tuple2<>(s.getText(), s.getText().split(" ").length);
					}
				});

		// printing tweet along with it's number of word
		tweetWordCount.print();

		// Part A - Question 2 --
		// Extract hashtags in each tweet
		JavaDStream<String> words = tweetStream.flatMap(new FlatMapFunction<Status, String>() {
			@Override
			public Iterator<String> call(Status s) {
				return Arrays.asList(s.getText().split(" ")).iterator();
			}
		});

		// extracting the hashtags from the tweet
		JavaDStream<String> hashTags = words.filter(new Function<String, Boolean>() {
			@Override
			public Boolean call(String word) {
				// returns the words which starts with '#'
				return word.startsWith("#");
			}
		});

		// displaying the hash tags in a tweet
		hashTags.print();

		// Part-A Question 3 ---
		// Question 3-a
		// Taking average number of characters per tweet.
		tweetCharCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				// takes number of elements/rows(tweet) in the rdd.
				long count = rdd.count();

				if (count == 0)
					return;

				// passing number of characters from the rdd using map, and counting number of
				// character per tweet and assigns value to the variable 'total'
				int total = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						return arg0 + arg1;
					}
				});

				// printing average characters in a tweet
				System.out.println("Average characters in a tweet: " + total / count);

			}
		});

		// Taking average number of words per tweet
		tweetWordCount.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
			public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
				// takes number of elements/rows(tweet) in the rdd.
				long count = rdd.count();

				if (count == 0)
					return;

				// passing number of characters from the rdd using map, and counting number of
				// words per tweet and assigns value to the variable 'total'
				int total = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer arg0, Integer arg1) throws Exception {
						return arg0 + arg1;
					}
				});

				// printing average words in a tweet
				System.out.println("Average words in a tweet: " + total / count);
			}
		});

		// Part-A Question-3b
		// creating pair of hashtag word with 1, it is used to reduce or count the
		// frequency of that word.
		JavaPairDStream<String, Integer> hashTagMapper = hashTags
				.mapToPair(new PairFunction<String, String, Integer>() {
					public Tuple2<String, Integer> call(String s) {
						// leave out the # character and paired with value 1.
						return new Tuple2<>(s.substring(1), 1);
					}
				});

		// counting the frequency of hashtags, and sorting it in descending order using
		// sortByKey in transformToPair and printing top 10 hashtag words
		hashTagMapper.reduceByKey(new Function2<Integer, Integer, Integer>() {
			@Override
			public Integer call(Integer a, Integer b) {
				return a + b;
			}
		}) // tuple of (hashtag word, frequency)
				.mapToPair(Tuple2::swap) // tuple of (frequency, hashtag word)
				.transformToPair(s -> s.sortByKey(false)) // sort by frequency
				.print(10); // display top 10

		// Part-A Question 3c -----
		// Printing average number of characters per tweet for the last 5 minutes with a
		// window of 30 seconds
		tweetCharCount.window(Durations.minutes(5), Durations.seconds(30))
				.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
					public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
						long count = rdd.count();
						if (count == 0)
							return;

						// passing number of characters from the rdd using map, and counting number of
						// characters per tweet and assigns value to the variable 'total'
						int total = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {
							@Override
							public Integer call(Integer arg0, Integer arg1) throws Exception {
								return arg0 + arg1;
							}
						});
						// printing average characters in a tweet with 30 seconds window
						System.out.println("Average Characters in a tweet with 30 seconds window: " + total / count);
					}
				});

		// Printing average number of words per tweet for the last 5 minutes with a
		// window of 30 seconds
		tweetWordCount.window(Durations.minutes(5), Durations.seconds(30))
				.foreachRDD(new VoidFunction<JavaPairRDD<String, Integer>>() {
					public void call(JavaPairRDD<String, Integer> rdd) throws Exception {
						// takes number of elements/rows(tweet) in the rdd.
						long count = rdd.count();

						if (count == 0)
							return;

						// passing number of characters from the rdd using map, and counting number of
						// words per tweet and assigns value to the variable 'total'
						int total = rdd.map(x -> x._2()).reduce(new Function2<Integer, Integer, Integer>() {
							@Override
							public Integer call(Integer arg0, Integer arg1) throws Exception {
								return arg0 + arg1;
							}
						});

						// printing average words in a tweet with 30 seconds window
						System.out.println("Average words in a tweet in 30 seconds window: " + total / count);
					}
				});

		// printing frequency and hashtag of top 10 hashtag for the last 5 minutes with
		// a
		// window of 30 seconds
		JavaPairDStream<String, Integer> hashTagTotalInWindow = hashTagMapper
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer a, Integer b) {
						return a + b;
					}
				}, Durations.minutes(5), Durations.seconds(30));

		hashTagTotalInWindow // tuple of (hashtag word, frequency)
				.mapToPair(Tuple2::swap) // tuple of (frequency, hashtag word)
				.transformToPair(s -> s.sortByKey(false)) // sort by frequency
				.print(10); // display top 10

		jssc.start(); // start the execuation of the streams
		try {
			jssc.awaitTermination(); // it just waits for the termination signal from user
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
