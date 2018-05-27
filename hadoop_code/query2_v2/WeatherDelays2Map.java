package query2_v2;

import java.io.IOException;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WeatherDelays2Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, WeatherDelayType> {

	@Override
	public void map(LongWritable arg0, Text arg1, OutputCollector<Text, WeatherDelayType> arg2, Reporter arg3)
			throws IOException {
		String[] line = arg1.toString().split(",");
		if (!(line[0].equals("Year") || line[25].equals("NA") || line[14].equals("NA"))) {
			int delay = Integer.parseInt(line[25]);
			int count = Integer.parseInt(line[14]);
			String key = null;
			Calendar calendar = new GregorianCalendar(Integer.parseInt(line[0]), Integer.parseInt(line[1]) - 1,
					Integer.parseInt(line[2]));
			int week = calendar.get(Calendar.WEEK_OF_YEAR);
			int year;
			if (line[1].equals("1") && (week == 52 || week == 53)) {
				year = Integer.parseInt(line[0]) - 1;
				key = year + "-" + week;
			} else if (line[1].equals("12") && (week == 1)){
				year = Integer.parseInt(line[0]) + 1;
				key = year + "-" + week;
			}else {
				key = line[0] + "-" + week;
			}
			arg2.collect(new Text(key), new WeatherDelayType(delay, count));
		}
	}

}
