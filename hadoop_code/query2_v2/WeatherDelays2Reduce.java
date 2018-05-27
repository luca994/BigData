package query2_v2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WeatherDelays2Reduce extends MapReduceBase implements Reducer<Text, WeatherDelayType, Text, DoubleWritable> {

	@Override
	public void reduce(Text arg0, Iterator<WeatherDelayType> arg1, OutputCollector<Text, DoubleWritable> arg2, Reporter arg3)
			throws IOException {
		double delayedFlight = 0;
		int totalFlight = 0;
		while (arg1.hasNext()) {
			WeatherDelayType t = arg1.next();
			int delay = t.getDelay().get();
			int count = t.getCount().get();
			if (delay > 0) 
				delayedFlight += 1;
			if (count > 0)
				totalFlight += 1;
			
		}
		arg2.collect(arg0, new DoubleWritable((delayedFlight / totalFlight) * 100));
	}

}
