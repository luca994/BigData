package bd_project.query4;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PenaltyMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, DoubleWritable> arg2, Reporter arg3)
			throws IOException {
		
		String[] line = arg1.toString().split(",");	
		if(!(line[0].equals("Year") || line[15].equals("NA") || line[14].equals("NA"))) {
			
			String originAirport = line[16];
			String destAirport = line[17];
			double penaltyOrigin = 0;
			double penaltyDest = 0;
			if(Integer.parseInt(line[15])>15)
				penaltyOrigin = 1;
			if(Integer.parseInt(line[14])>15)
				penaltyDest = 0.5;
			
			String yearWeek = null;
			Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1,
					Integer.parseInt(line[2]));
			int week = calendar.get(Calendar.WEEK_OF_YEAR);
			int year;
			if (line[1].equals("1") && (week == 52 || week == 53)) {
				year = Integer.parseInt(line[0]) - 1;
				yearWeek = year + "-" + week;
			} else if (line[1].equals("12") && (week == 1)){
				year = Integer.parseInt(line[0]) + 1;
				yearWeek = year + "-" + week;
			}
			else {
				yearWeek = line[0] + "-" + week;
			}
			String keyOrigin = yearWeek+"-"+originAirport;
			String keyDest = yearWeek+"-"+destAirport;
			arg2.collect(new Text(keyOrigin), new DoubleWritable(penaltyOrigin));
			arg2.collect(new Text(keyDest), new DoubleWritable(penaltyDest));
			
		}
	}
}
