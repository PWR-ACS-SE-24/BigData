import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

public class DailyCountryWeather2 {
    public static class DailyCountryWeather2Mapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            String country = fields[0];
            String date = fields[1];
            String temperatureC = fields.length >= 3 ? fields[2] : "";
            String precipitationMm = fields.length == 4 ? fields[3] : "";

            String outKey = country + "!@#" + date;
            String outValue = temperatureC + "!@#" + precipitationMm;
            context.write(new Text(outKey), new Text(outValue));
        }
    }

    public static class DailyCountryWeather2Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String[] keyParts = key.toString().split("!@#");
            String country = keyParts[0];
            String date = keyParts[1];

            double temperatureSum = 0.0;
            int temperatureCount = 0;
            double precipitationSum = 0.0;
            int precipitationCount = 0;

            for (Text value : values) {
                String[] valueParts = value.toString().split("!@#");
                String temperatureC = valueParts.length >= 1 ? valueParts[0] : "";
                if (!temperatureC.isEmpty()) {
                    temperatureSum += Double.parseDouble(temperatureC);
                    temperatureCount++;
                }
                String precipitationMm = valueParts.length == 2 ? valueParts[1] : "";
                if (!precipitationMm.isEmpty()) {
                    precipitationSum += Double.parseDouble(precipitationMm);
                    precipitationCount++;
                }
            }

            if (temperatureCount == 0) {
                return;
            }

            double averageTemperatureC = temperatureSum / temperatureCount;
            double averagePrecipitationMm = precipitationCount > 0 ? precipitationSum / precipitationCount : 0.0;

            String output = String.join(",",
                    country,
                    date,
                    String.format("%.2f", averageTemperatureC),
                    String.format("%.2f", averagePrecipitationMm)
            );
            context.write(NullWritable.get(), new Text(output));
        }
    }

    public static int run(BenchmarkConfig config, String inputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        config.setup(conf, inputPath);
        Job job = Job.getInstance(new Configuration(), "DailyCountryWeather2");

        job.setJarByClass(DailyCountryWeather2.class);
        job.setMapperClass(DailyCountryWeather2Mapper.class);
        job.setReducerClass(DailyCountryWeather2Reducer.class);
        job.setNumReduceTasks(config.reducers);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        int status = job.waitForCompletion(true) ? 0 : 1;

        config.teardown(conf, inputPath);

        return status;
    }
}
