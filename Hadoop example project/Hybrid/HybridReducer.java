package hybrid;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import common.MyPair;
import common.Tools;

public class HybridReducer extends Reducer<MyPair, IntWritable, Text, Text> {

	int currentSum;
	String currentTerm;
	MapWritable currentStripe;
	
	@Override
	protected void setup(Reducer<MyPair, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		super.setup(context);
		currentTerm = "";
		currentStripe = new MapWritable();
		currentSum = 0;
	}

	@Override
	protected void reduce(MyPair key, Iterable<IntWritable> values, Reducer<MyPair, IntWritable, Text, Text>.Context context) throws IOException,
			InterruptedException {
		if(!key.getFirst().equals(currentTerm)){
			if(currentStripe.size()>0){
				for(Writable k : currentStripe.keySet()){
					double newValue = (double)((IntWritable)currentStripe.get(k)).get()/currentSum;
					currentStripe.put(k, new DoubleWritable(newValue));
				}
				context.write(new Text(currentTerm), Tools.toText(currentStripe));
			}
			
			currentTerm = key.getFirst();
			currentSum = 0;
			currentStripe = new MapWritable();
		}
		Text keyRight = new Text(key.getSecond());
		if(!currentStripe.containsKey(keyRight.toString())){
			currentStripe.put(keyRight, new IntWritable(0));
		}
		for(IntWritable v : values){
			int value = v.get();
			currentSum += value;
			int newValue = ((IntWritable)currentStripe.get(keyRight)).get()+value;
			currentStripe.put(new Text(key.getSecond()), new IntWritable(newValue));
		}
	}

	@Override
	protected void cleanup(Reducer<MyPair, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		for(Writable k:currentStripe.keySet()){
			double newValue = (double)((IntWritable)currentStripe.get(k)).get()/currentSum;
			currentStripe.put(k, new DoubleWritable(newValue));
		}
		context.write(new Text(currentTerm), Tools.toText(currentStripe));
		super.cleanup(context);
	}
}