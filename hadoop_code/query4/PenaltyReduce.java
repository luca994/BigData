package bd_project.query4;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class PenaltyReduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

	@Override
	public void reduce(Text arg0, Iterator<DoubleWritable> arg1, OutputCollector<Text, DoubleWritable> arg2,
			Reporter arg3) throws IOException {
		
		double penalty = 0;
		while(arg1.hasNext()) {
			DoubleWritable temp = arg1.next();
			penalty+=temp.get();
		}
		arg2.collect(arg0, new DoubleWritable(penalty));
	}

}
