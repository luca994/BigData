package query2_v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class WeatherDelayType implements Writable {

	private IntWritable delay;
	private IntWritable count;
	
	public WeatherDelayType() {
		clear();
	}
	
	public WeatherDelayType(int delay, int count) {
		this.delay = new IntWritable(delay);
		this.count = new IntWritable(count);
	}
	
	public void clear() {
		delay = new IntWritable();
		count = new IntWritable();
	}
	
	public IntWritable getDelay() {
		return delay;
	}

	public IntWritable getCount() {
		return count;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		clear();
		delay.readFields(arg0);
		count.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		delay.write(arg0);
		count.write(arg0);
	}

}
