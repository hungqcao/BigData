package stripes;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import common.Tools;

public class StripesReducer extends Reducer<Text, MapWritable, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<MapWritable> values, Reducer<Text, MapWritable, Text, Text>.Context context) throws IOException,
			InterruptedException {
		MapWritable map = new MapWritable();
		int marginal = 0;
		for(MapWritable v:values){
			marginal += sum(map, v);
		}
		for(Writable k : map.keySet()){
			int value = ((IntWritable) map.get(k)).get();
			float freq = (float)value/marginal;
			map.put(k, new FloatWritable(freq));
		}
		context.write(key, Tools.toText(map));
	}
	
	private int sum( MapWritable s, MapWritable a){
		int result = 0;
		for(Writable key : a.keySet()){
			int addedValue = ((IntWritable) a.get(key)).get();
			result += addedValue;
			if(s.containsKey(key)){
				IntWritable intWrit = (IntWritable) s.get(key);
				int newValue = ((IntWritable)s.get(key)).get() + addedValue;
				intWrit.set(newValue);
			}else{				
				s.put(key, new IntWritable(addedValue));
			}
		}
		return result;
	}

}