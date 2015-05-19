package pair;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import common.MyPair;
import common.Tools;

public class PairMapper extends Mapper<Object, Text, MyPair, IntWritable> {
	private static final int FLUSH_SIZE = 1000;
	private Map<MyPair, Integer> map;

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		Map<MyPair, Integer> map = getMap();
		MyPair pairObj = null;
		MyPair countObj = null;
		String[] words = value.toString().split(" ");
		for (int i = 0; i < words.length; i++) {
			String[] neighbors = Tools.getNeighbor(words, i);
			countObj = new MyPair(words[i], "0");
			if(!map.containsKey(countObj)){
				map.put(countObj, 0);
			}
			for (int j = 0; j < neighbors.length; j++) {
				pairObj = new MyPair(words[i], neighbors[j]);
				if (map.containsKey(pairObj)) {
					int total = map.get(pairObj).intValue();
					map.put(pairObj, total + 1);
				} else {
					map.put(pairObj, 1);
				}
				int totalOfCountObj = map.get(countObj).intValue();
				map.put(countObj, totalOfCountObj + 1);
			}
		}
		flush(context, false);
	}

	private void flush(Context context, boolean force) throws IOException,
			InterruptedException {
		Map<MyPair, Integer> map = getMap();
		if (!force) {
			int size = map.size();
			if (size < FLUSH_SIZE)
				return;
		}

		Iterator<Map.Entry<MyPair, Integer>> it = map.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry<MyPair, Integer> entry = it.next();
			MyPair sKey = entry.getKey();
			int total = entry.getValue().intValue();
			context.write(sKey, new IntWritable(total));
		}

		map.clear(); // make sure to empty map
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		flush(context, true); // force flush no matter what at the end
	}

	public Map<MyPair, Integer> getMap() {
		if (null == map) // lazy loading
			map = new HashMap<MyPair, Integer>();
		return map;
	}
}