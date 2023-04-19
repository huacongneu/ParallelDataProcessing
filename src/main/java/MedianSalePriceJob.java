import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
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

public class MedianSalePriceJob {

  public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    private Text state = new Text();
    private DoubleWritable medianSalePrice = new DoubleWritable();
    private DataParser dataParser;

    // Emit <State, Median Sale Price>
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      dataParser = new DataParser();
      Record record;
      try {
        record = dataParser.getSingleRecord(value.toString());
      } catch (CsvValidationException e) {
        throw new RuntimeException(e);
      }

      if (isValidRecord(record)) {
        state.set(record.getState());
        medianSalePrice.set(record.getSalePrice());
        context.write(state, medianSalePrice);
      }
    }

    private boolean isValidRecord(Record record) {
      // 1. the record should be valid
      if (record.getYear().equals("invalid") || record.getState().equals("invalid") ||
          record.getPropertyTypeId().equals("invalid") || record.getSalePrice() == -100) {
        return false;
      }

      // 2. make a filter: the record should be for single family house and in 2021
      if (!record.getPropertyTypeId().equals("6") || !record.getYear().equals("2021")) {
        return false;
      }

      return true;
    }
  }

  public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    // <MedianSalePrice, StateCode>
    private TreeMap<Double, String> priceToState = new TreeMap<>(Collections.reverseOrder());

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
      priceToState.put(medianSalePrice, key.toString());
      if (priceToState.size() > 10) {
        priceToState.remove(priceToState.lastKey());
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // Output the top 10 states with the highest median sale price
      for (Double price : priceToState.keySet()) {
        context.write(new Text(priceToState.get(price)), new DoubleWritable(price));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Top 10 states with the highest median sale price");
    job.setJarByClass(MedianSalePriceJob.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}