import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class Covid19_1 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private IntWritable newCases = new IntWritable(1);
        private Text location = new Text();
        private static boolean includeWorld;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            includeWorld = context.getConfiguration().getBoolean("includeWorld", false);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                String[] split = line.split(",");
                if (split.length == 4 && split[0].startsWith("2020")
                        && (includeWorld || (!"world".equalsIgnoreCase(split[1]) && !"international".equalsIgnoreCase(split[1])))) {
                    location.set(split[1]);
                    newCases.set(Integer.parseInt(split[2]));
                    context.write(location, newCases);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 3) {
            Configuration conf = new Configuration();
            conf.setBoolean("includeWorld", "true".equalsIgnoreCase(args[1]));

            Job job = Job.getInstance(conf, "Covid19_1");
            job.setJarByClass(Covid19_1.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[2]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            System.out.println("Usage: hadoop jar Covid19.jar Covid19_1 path/to/input (true|false) path/to/output");
        }
    }
}