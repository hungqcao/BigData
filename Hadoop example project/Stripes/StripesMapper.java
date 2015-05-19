package stripes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import common.Tools;

public class StripesMapper extends Mapper<Object, Text, Text, MapWritable> {
	private static final int FLUSH_SIZE = 1000;
	private Map<String, MapWritable> H;
	
	@Override
	protected void setup(Mapper<Object, Text, Text, MapWritable>.Context context)
			throws IOException, InterruptedException {
		H = new HashMap<String, MapWritable>();
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(Object key, Text value, Mapper<Object, Text, Text, MapWritable>.Context context)
			throws IOException, InterruptedException {
		Map<String, MapWritable> map = getMap();
		String[] words = value.toString().split(" ");
		for (int i = 0; i < words.length; i++) {
			String[] neighbors = Tools.getNeighbor(words, i);
			if(!map.containsKey(words[i])){
				map.put(words[i], new MapWritable());
			}
			for (int j = 0; j < neighbors.length; j++) {
				MapWritable stripe = map.get(words[i]);
				Text neighbor = new Text(neighbors[j]);
				if(stripe.containsKey((Writable) neighbor)){
					IntWritable writer = ((IntWritable) stripe.get(neighbor));
					writer.set(writer.get() + 1);
				} else {
					// put new if new neighbor
					stripe.put(neighbor, new IntWritable(1));
				}
			}
		}
		flush(context, false);
	}

	private void flush(Mapper<Object, Text, Text, MapWritable>.Context context, boolean force) throws IOException,
			InterruptedException {
		Map<String, MapWritable> map = getMap();
		if (!force) {
			int size = map.size();
			if (size < FLUSH_SIZE)
				return;
		}

		Iterator<Map.Entry<String, MapWritable>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<String, MapWritable> entry = it.next();
			String sKey = entry.getKey();
			context.write(new Text(sKey), entry.getValue());
		}

		map.clear(); // make sure to empty map
	}

	protected void cleanup(Mapper<Object, Text, Text, MapWritable>.Context context) throws IOException,
			InterruptedException {
		flush(context, true); // force flush no matter what at the end
	}

	public Map<String, MapWritable> getMap() {
		if (null == H) // lazy loading
			H = new HashMap<String, MapWritable>();
		return H;
	}
}