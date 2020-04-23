import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

public class SparkCovid19_1 {

    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    public static void main(String[] args) {
        if (args.length == 4) {
            SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkCovid19_1");
            JavaSparkContext sc = new JavaSparkContext(conf);

            Date startDate, endDate;
            try {
                startDate = simpleDateFormat.parse(args[1]);
                endDate = simpleDateFormat.parse(args[2]);
            } catch (ParseException e) {
                System.out.println("Invalid date format. Use yyyy-MM-dd.");
                e.printStackTrace();
                return;
            }

            JavaRDD<String> textFile = sc.textFile(args[0]);
            JavaPairRDD<String, Integer> counts = textFile
                    .mapToPair(value -> {
                        String[] split = value.split(",");
                        Date date;
                        try {
                            date = simpleDateFormat.parse(split[0]);
                        } catch (ParseException | IndexOutOfBoundsException e) {
                            return null;
                        }
                        if (split.length == 4
                                && (date.before(endDate) || date.equals(endDate))
                                && (date.after(startDate) || date.equals(startDate))) {
                            String location = split[1];
                            int newDeaths = Integer.parseInt(split[3]);
                            return new Tuple2<>(location, newDeaths);
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .reduceByKey(Integer::sum);

            counts.saveAsTextFile(args[3]);
        } else {
            System.out.println("Usage: spark-submit --class SparkCovid19_1 Covid19.jar fully-qualified-path/to/input start_date(yyyy-MM-dd) end_date(yyyy-MM-dd) fully-qualified-path/to/output");
        }
    }
}
