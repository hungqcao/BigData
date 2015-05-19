package sparkexample;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import common.Tools;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class SparkExample{
	private static final FlatMapFunction<String, Tuple2<String, String>> WORDS_EXTRACTOR = new FlatMapFunction<String, Tuple2<String, String>>() {
		@Override
		public Iterable<Tuple2<String, String>> call(String s) throws Exception {
			ArrayList<Tuple2<String, String>> result = new ArrayList<Tuple2<String, String>>();
			String[] words = s.toString().split(" ");
			for (int i = 0; i < words.length; i++) {
				String[] neighbors = Tools.getNeighbor(words, i);
				for (int j = 0; j < neighbors.length; j++) {
					result.add(new Tuple2<String, String>(words[i],
							neighbors[j]));
				}
			}
			return result;
		}
	};
	
	private static final PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer> WORDS_MAPPER = new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
		@Override
		public Tuple2<Tuple2<String, String>, Integer> call(
				Tuple2<String, String> s) throws Exception {
			return new Tuple2<Tuple2<String, String>, Integer>(s, 1);
		}
	};
	
	private static final PairFunction<Tuple2<String, String>, String, Integer> COUNT_MAPPER = new PairFunction<Tuple2<String,String>, String, Integer>() {
		
		@Override
		public Tuple2<String, Integer> call(Tuple2<String, String> t)
				throws Exception {
			return new Tuple2<String, Integer>(t._1, 1);
		}
	};

	private static final Function2<Integer, Integer, Integer> WORDS_REDUCER = new Function2<Integer, Integer, Integer>() {
		@Override
		public Integer call(Integer a, Integer b) throws Exception {
			return a + b;
		}
	};
	private static final Comparator<Tuple2<Tuple2<String, String>, Double>> compareTuple = new Comparator<Tuple2<Tuple2<String, String>, Double>>() {

		@Override
		public int compare(Tuple2<Tuple2<String, String>, Double> arg0,
				Tuple2<Tuple2<String, String>, Double> arg1) {
			if (arg0._1._1.equals(arg1._1._1)) {
				return arg0._1._2.compareTo(arg1._1._2);
			} else {
				return arg0._1._1.compareTo(arg1._1._1);
			}
		}
	};

	public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Please provide the input file full path as argument");
      System.exit(0);
    }

    SparkConf conf = new SparkConf().setAppName("sparkexample.SparkExample").setMaster("local");
    JavaSparkContext context = new JavaSparkContext(conf);

    JavaRDD<String> file = context.textFile(args[0]);
    JavaRDD<Tuple2<String, String>> words = file.flatMap(WORDS_EXTRACTOR);
    JavaPairRDD<Tuple2<String, String>, Integer> pairs = words.mapToPair(WORDS_MAPPER);
    JavaPairRDD<String, Integer> pairsCount = words.mapToPair(COUNT_MAPPER);
    List<Tuple2<Tuple2<String, String>, Integer>> counter = pairs.reduceByKey(WORDS_REDUCER).collect();
    List<Tuple2<Tuple2<String, String>, Double>> finalResult = new ArrayList<Tuple2<Tuple2<String,String>,Double>>();
    List<Tuple2<String, Integer>> counterCount = pairsCount.reduceByKey(WORDS_REDUCER).collect();
    counter.forEach(f -> {
		Tuple2<String, Integer> tuple = Tools.myFindTupleIn(counterCount, f._1._1);
    	finalResult.add(new Tuple2<Tuple2<String,String>, Double>(f._1, (double)f._2 / tuple._2));
    });
    finalResult.sort(compareTuple);
    JavaPairRDD<Tuple2<String, String>, Double> finalFile = context.parallelizePairs(finalResult);
    finalFile.saveAsTextFile(args[1]);
  }
}