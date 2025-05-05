import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ChartsDailySum {
 public static class ChartsDailySumMapper extends Mapper<Object, Text, Text, LongWritable> {
    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      String region = fields[0];
      String date = fields[1];
      String streams = fields[3];

      String outKey = region + "!@#" + date;
      long streamsCount = Long.parseLong(streams);
      context.write(new Text(outKey), new LongWritable(streamsCount));
    }
  }

  public static class ChartsDailySumReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      String[] keyParts = key.toString().split("!@#");
      String region = keyParts[0];
      String date = keyParts[1];

      long sum = 0;
      for (LongWritable value : values) {
        sum += value.get();
      }

      String output = String.join(",",
        region,
        date,
        String.valueOf(sum)
      );

      context.write(NullWritable.get(), new Text(output));
    }
  }

  public static int run(String inputPath, String outputPath) throws Exception {
    Job job = Job.getInstance(new Configuration(), "ChartsDailySum");

    job.setJarByClass(ChartsDailySum.class);
    job.setMapperClass(ChartsDailySumMapper.class);
    job.setReducerClass(ChartsDailySumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(inputPath));

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}
