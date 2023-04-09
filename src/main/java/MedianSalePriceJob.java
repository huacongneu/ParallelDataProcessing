import com.opencsv.CSVParserBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVParser;

public class MedianSalePriceJob {

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text state = new Text();
    private DoubleWritable medianSalePrice = new DoubleWritable();

    // Emit <State, Median Sale Price>
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      BufferedReader reader = new BufferedReader(new StringReader(line));
      CSVParser csvParser = new CSVParserBuilder().withSeparator(',').build();
      String[] parts;
      while ((parts = csvParser.parseLine(reader.readLine())) != null) {
        // Extract relevant attributes from the record
        String yearStr = parts[1].substring(0, 4);
        String stateCode = parts[10];
        String propertyTypeId = parts[12];
        double salePrice = Double.parseDouble(parts[13]);

        // Check if record is for Single Family Home and 2021
        if (propertyTypeId.equals("6") && yearStr.equals("2021")) {
          state.set(stateCode);
          medianSalePrice.set(salePrice);
          context.write(state, medianSalePrice);
        }
      }
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // <MedianSalePrice, StateCode>
    private TreeMap<Double, String> top10States = new TreeMap<>(Collections.reverseOrder());

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
      double totalSalePrice = 0.0;
      int count = 0;

      // Calculate median sale price for this state
      for (DoubleWritable val : values) {
        totalSalePrice += val.get();
        count++;
      }

      double medianSalePrice = totalSalePrice / count;

      // Add state to the top 10 if it has a higher median sale price than the lowest median sale price in the current top 10
      top10States.put(medianSalePrice, key.toString());
      if (top10States.size() > 10) {
        top10States.remove(top10States.lastKey());
      }
    }

    public void cleanup(Context context) throws IOException, InterruptedException {
      // Output top 10 states with highest median sale price
      for (double price : top10States.keySet()) {
        String state = top10States.get(price);
        context.write(new Text(state), new DoubleWritable(price));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Median Sale Price by State");
    job.setJarByClass(MedianSalePriceJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
