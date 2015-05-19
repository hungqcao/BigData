package common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyPair implements WritableComparable<MyPair>{
	private String first;
	private String second;
	public String getFirst() {
		return first;
	}
	public void setFirst(String first) {
		this.first = first;
	}
	public String getSecond() {
		return second;
	}
	public void setSecond(String second) {
		this.second = second;
	}
	public MyPair(String first, String second) {
		super();
		this.first = first;
		this.second = second;
	}
	
	public MyPair() {
		super();
	}
	
	@Override
	public String toString() {
		return String.format("%s, %s", first, second);
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((first == null) ? 0 : first.hashCode());
		result = prime * result + ((second == null) ? 0 : second.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MyPair other = (MyPair) obj;
		if (first == null) {
			if (other.first != null)
				return false;
		} else if (!first.equals(other.first))
			return false;
		if (second == null) {
			if (other.second != null)
				return false;
		} else if (!second.equals(other.second))
			return false;
		return true;
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		first = arg0.readUTF();
		second = arg0.readUTF();
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeUTF(first);
		arg0.writeUTF(second);
	}
	@Override
	public int compareTo(MyPair arg0) {
		// TODO Auto-generated method stub
		if(first.equals(arg0.first)){
			return second.compareTo(arg0.second);
		}else{
			return first.compareTo(arg0.first);
		}
	}
	
}
