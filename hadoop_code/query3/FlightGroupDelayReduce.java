package bd_project.query3;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FlightGroupDelayReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text arg0, Iterator<IntWritable> arg1, OutputCollector<Text, DoubleWritable> arg2,
			Reporter arg3) throws IOException {
		
		int totFlights=0;
		double halveDelayFlights=0;
		while(arg1.hasNext()) {
			IntWritable halveDelay = arg1.next();
			halveDelayFlights+=halveDelay.get();
			totFlights+=1;
		}
		arg2.collect(arg0, new DoubleWritable((halveDelayFlights/totFlights)*100));
	}

}
