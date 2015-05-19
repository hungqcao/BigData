package pair;

import org.apache.hadoop.mapreduce.Partitioner;

import common.MyPair;

public class PairPartitioner extends Partitioner<MyPair, Integer>{

	@Override
	public int getPartition(MyPair arg0, Integer arg1, int arg2) {
		// TODO Auto-generated method stub
		return arg0.getFirst().hashCode() % arg2;
	}

}
