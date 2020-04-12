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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.StringTokenizer;

public class Covid19_2 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        private IntWritable newDeaths = new IntWritable(1);
        private Text location = new Text();
        private static Date startDate, endDate;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            String[] dates = context.getConfiguration().getStrings("dates");
            try {
                startDate = simpleDateFormat.parse(dates[0]);
                endDate = simpleDateFormat.parse(dates[1]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String line = itr.nextToken();
                String[] split = line.split(",");
                Date date;
                try {
                    date = simpleDateFormat.parse(split[0]);
                } catch (ParseException | IndexOutOfBoundsException e) {
                    date = Calendar.getInstance().getTime();
                }
                if (split.length == 4
                        && (date.before(endDate) || date.equals(endDate))
                        && (date.after(startDate) || date.equals(startDate))) {
                    location.set(split[1]);
                    newDeaths.set(Integer.parseInt(split[3]));
                    context.write(location, newDeaths);
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
        if (args.length == 4) {
            Configuration conf = new Configuration();
            conf.setStrings("dates", args[1], args[2]);

            Job job = Job.getInstance(conf, "Covid19_2");
            job.setJarByClass(Covid19_2.class);
            job.setMapperClass(TokenizerMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[3]));

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } else {
            System.out.println("Usage: hadoop jar Covid19.jar Covid19_2 path/to/input start_date(yyyy-MM-dd) end_date(yyyy-MM-dd) path/to/output");
        }
    }
}