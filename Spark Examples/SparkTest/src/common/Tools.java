package common;

import java.util.ArrayList;
import java.util.List;

import scala.Tuple2;

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

	public static Tuple2<String, Integer> myFindTupleIn(List<Tuple2<String, Integer>> source, String input){
		for (Tuple2<String, Integer> tuple2 : source) {
			if(tuple2._1.equals(input))
				return tuple2;
		}
		return null;
	}
}
