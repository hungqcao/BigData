package pair;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import common.MyPair;

public class PairReducer extends Reducer<MyPair, IntWritable, MyPair, FloatWritable> {

	private int marginal;

	@Override
	public void reduce(MyPair key, Iterable<IntWritable> values, Context output)
			throws IOException, InterruptedException {
		
			int voteCount = 0;
			for (IntWritable value : values) {
				voteCount += value.get();
			}
			if( key.getSecond().equals("0")){
				marginal = voteCount;
			}
			else {
				float freq = (float) voteCount/marginal;
				output.write(key, new FloatWritable(freq));
			}
	}
}