import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Covid19_3 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable newCases = new IntWritable(1);
        private Text location = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] split = value.toString().split(",");
                if (split.length == 4) {
                    location.set(split[1]);
                    newCases.set(Integer.parseInt(split[2]));
                    context.write(location, newCases);
                }
            } catch (NumberFormatException e) { // Ignore invalid lines including header
                System.out.println("Exception for line: " + value.toString());
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        private static Map<String, Long> locationToPopulationMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            URI[] cacheFiles = context.getCacheFiles();

            // Read cached population file into locationToPopulation HashMap
            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem fileSystem = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                String line;
                while ((line = br.readLine()) != null) {
                    try {
                        String[] split = line.split(",");
                        locationToPopulationMap.put(split[1], Long.parseLong(split[4]));
                    } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                        // Ignore invalid lines including header
                        System.out.println("Exception for line: " + line);
                    }
                }
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // If location is present in cached file hashmap
            if (locationToPopulationMap.containsKey(key.toString())) {
                int sum = 0;
                // Sum the new cases for location
                for (IntWritable val : values) {
                    sum += val.get();
                }
                // Compute new cases per million population
                result.set(sum * 1_000_000.0 / locationToPopulationMap.get(key.toString()));
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            Configuration conf = new Configuration();

            Job job = Job.getInstance(conf, "Covid19_3");
            job.addCacheFile(new Path(args[1]).toUri()); // Add population file to Distributed Cache
            job.setJarByClass(Covid19_3.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            System.out.println("Usage: hadoop jar Covid19.jar Covid19_3 path/to/input fully_qualified_path/to/cache_file path/to/output");
        }
    }
}