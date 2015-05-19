package common;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Tools {
	public static String[] getNeighbor(String[] words, int index) {

		List<String> neighbor = new ArrayList<String>();

		for (int i = index + 1; i < words.length; i++) {
			if (!words[index].equals(words[i])) {
				neighbor.add(words[i]);
			} else
				break;
		}

		return neighbor.toArray(new String[neighbor.size()]);

	}
	public static Text toText(MapWritable map) {
		StringBuilder sb = new StringBuilder("");
		sb.append("[");
		for (Writable k : map.keySet()) {
			sb.append("(" + k)
			.append(",")
			.append(map.get(k) + ")");
		}
		sb.append("]");
		return new Text(sb.toString());
	}
}
