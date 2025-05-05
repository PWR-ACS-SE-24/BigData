import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ChartsFmt {
  public static class ChartsFmtMapper extends Mapper<Object, Text, NullWritable, Text> {
    @Override
    protected void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      if (line.equals("title,rank,date,artist,url,region,chart,trend,streams")) {
        return;
      }

      String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

      if (!fields[6].equals("top200")) {
        return;
      }

      String output = String.join(",",
        fields[5], // region
        fields[2], // date
        fields[4].substring("https://open.spotify.com/track/".length()), // track_id
        fields[8] // streams
      );

      context.write(NullWritable.get(), new Text(output));
    }
  }

  public static int run(BenchmarkConfig config, String inputPath, String outputPath) throws Exception {
    System.err.println(config);

    Configuration conf = new Configuration();
    conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(config.splitMb * 1024 * 1024));
    conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(config.splitMb * 1024 * 1024));
    Job job = Job.getInstance(conf, "ChartsFmt");

    FileSystem fs = FileSystem.get(conf);
    fs.setReplication(new Path(inputPath), (short) config.replication);

    job.setJarByClass(ChartsFmt.class);
    job.setMapperClass(ChartsFmtMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, new Path(inputPath));

    job.setOutputFormatClass(TextOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    int status = job.waitForCompletion(true) ? 0 : 1;

    fs.setReplication(new Path(inputPath), (short) 3);

    return status;
  }
}
