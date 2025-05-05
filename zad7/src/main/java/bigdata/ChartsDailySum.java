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
  public static enum Counters { MAPPER, REDUCER }

  public static class ChartsDailySumMapper extends Mapper<Object, Text, Text, LongWritable> {
    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      long startTime = System.nanoTime();
      String line = value.toString();
      String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      String region = fields[0];
      String date = fields[1];
      String streams = fields[3];

      String outKey = region + "!@#" + date;
      long streamsCount = Long.parseLong(streams);
      context.write(new Text(outKey), new LongWritable(streamsCount));
      long endTime = System.nanoTime();
      context.getCounter(Counters.MAPPER).increment(endTime - startTime);
    }
  }

  public static class ChartsDailySumReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long startTime = System.nanoTime();
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
      long endTime = System.nanoTime();
      context.getCounter(Counters.REDUCER).increment(endTime - startTime);
    }
  }

  public static int run(BenchmarkConfig config, String inputPath, String outputPath) throws Exception {
    Configuration conf = new Configuration();
    config.setup(conf, inputPath);
    Job job = Job.getInstance(conf, "ChartsDailySum");

    job.setJarByClass(ChartsDailySum.class);
    job.setMapperClass(ChartsDailySumMapper.class);
    job.setReducerClass(ChartsDailySumReducer.class);
    job.setNumReduceTasks(config.reducers);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(inputPath));

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    long startTime = System.nanoTime();
    int status = job.waitForCompletion(true) ? 0 : 1;
    long endTime = System.nanoTime();

    config.teardown(conf, inputPath);

    System.err.println(String.format("ChartsDailySum: %.3f ms", (endTime - startTime) / 1e6));
    System.err.println(String.format("Mapper time: %.3f ms", job.getCounters().findCounter(Counters.MAPPER).getValue() / 1e6));
    System.err.println(String.format("Reducer time: %.3f ms", job.getCounters().findCounter(Counters.REDUCER).getValue() / 1e6));

    return status;
  }
}
