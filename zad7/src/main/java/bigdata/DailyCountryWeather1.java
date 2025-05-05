import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.ArrayList;

public class DailyCountryWeather1 {
    public static class DailyCountryWeather1WeatherMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.equals("station_id,date,avg_temp_c,precipitation_mm")) {
                return;
            }
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String stationId = fields[0];
            String date = fields[1].substring(0, 10);
            String temperatureC = fields.length >= 3 ? fields[2] : "";
            String precipitationMm = fields.length == 4 ? fields[3] : "";

            // TODO
            // if (date.compareTo("2017-01-01") < 0 || date.compareTo("2021-12-31") > 0) {
            //     return;
            // }

            context.write(new Text(stationId), new Text(String.join("!@#", "WEATHER", date, temperatureC, precipitationMm)));
        }
    }

    public static class DailyCountryWeather1CityMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            if (line.equals("station_id,city_name,country,state,iso2,iso3,latitude,longitude")) {
                return;
            }
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String stationId = fields[0];
            String country = fields[2];

            context.write(new Text(stationId), new Text(String.join("!@#", "CITY", country)));
        }
    }

    public static class DailyCountryWeather1Reducer extends Reducer<Text, Text, NullWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String stationId = key.toString();
            String country = null;
            ArrayList<String> dates = new ArrayList<>();
            ArrayList<String> temperaturesC = new ArrayList<>();
            ArrayList<String> precipitationsMm = new ArrayList<>();

            for (Text value : values) {
                String[] fields = value.toString().split("!@#");
                if (fields[0].equals("WEATHER")) {
                    dates.add(fields[1]);
                    temperaturesC.add(fields.length >= 3 ? fields[2] : "");
                    precipitationsMm.add(fields.length == 4 ? fields[3] : "");
                } else if (fields[0].equals("CITY")) {
                    country = fields[1];
                }
            }

            if (country == null) {
                return;
            }

            for (int i = 0; i < dates.size(); i++) {
                String date = dates.get(i);
                String temperatureC = temperaturesC.get(i);
                String precipitationMm = precipitationsMm.get(i);

                String output = String.join(",",
                        country,
                        date,
                        temperatureC,
                        precipitationMm
                );

                context.write(NullWritable.get(), new Text(output));
            }
        }
    }

    public static int run(BenchmarkConfig config, String weatherInputPath, String cityInputPath, String outputPath) throws Exception {
        Configuration conf = new Configuration();
        config.setup(conf, weatherInputPath);
        config.setup(conf, cityInputPath);
        Job job = Job.getInstance(new Configuration(), "DailyCountryWeather1");

        job.setJarByClass(DailyCountryWeather1.class);
        MultipleInputs.addInputPath(job, new Path(weatherInputPath), TextInputFormat.class, DailyCountryWeather1WeatherMapper.class);
        MultipleInputs.addInputPath(job, new Path(cityInputPath), TextInputFormat.class, DailyCountryWeather1CityMapper.class);
        job.setReducerClass(DailyCountryWeather1Reducer.class);
        job.setNumReduceTasks(config.reducers);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        int status = job.waitForCompletion(true) ? 0 : 1;

        config.teardown(conf, weatherInputPath);
        config.teardown(conf, cityInputPath);

        return status;
    }
}
