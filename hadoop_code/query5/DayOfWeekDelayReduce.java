package query5;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DayOfWeekDelayReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable>{

	@Override
	public void reduce(Text arg0, Iterator<IntWritable> arg1, OutputCollector<Text, DoubleWritable> arg2, Reporter arg3)
			throws IOException {
		int tot = 0;
		double numDelay = 0;
		while(arg1.hasNext()) {
			IntWritable delay = arg1.next();
			numDelay += delay.get();
			tot++;
		}
		arg2.collect(arg0, new DoubleWritable((numDelay/tot)*100));
		
	}

}
