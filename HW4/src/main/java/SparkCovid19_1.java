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

            // Get the start and end date
            Date startDate, endDate;
            try {
                startDate = simpleDateFormat.parse(args[1]);
                endDate = simpleDateFormat.parse(args[2]);
            } catch (ParseException e) { // Exit if dates are in invalid format
                System.out.println("Invalid date format. Use yyyy-MM-dd.");
                e.printStackTrace();
                return;
            }

            JavaRDD<String> textFile = sc.textFile(args[0]); // read input file
            JavaPairRDD<String, Integer> counts = textFile
                    .mapToPair(value -> { // map the input to key value pair (location, new_deaths)
                        String[] split = value.split(",");
                        Date date;
                        try {
                            date = simpleDateFormat.parse(split[0]);
                        } catch (ParseException | IndexOutOfBoundsException e) {
                            // Ignore lines with invalid date including header
                            return null;
                        }
                        if (split.length == 4 // if date within specified interval
                                && (date.after(startDate) || date.equals(startDate))
                                && (date.before(endDate) || date.equals(endDate))) {
                            String location = split[1];
                            int newDeaths = Integer.parseInt(split[3]);
                            return new Tuple2<>(location, newDeaths);
                        } else {
                            // Ignore dates outside specified interval
                            return null;
                        }
                    })
                    .filter(Objects::nonNull) // filter out null values
                    .reduceByKey(Integer::sum) // sum the new_deaths for each location (key)
                    .sortByKey(); // sort output by location

            counts.saveAsTextFile(args[3]);
        } else {
            System.out.println("Usage: spark-submit --class SparkCovid19_1 Covid19.jar fully-qualified-path/to/input start_date(yyyy-MM-dd) end_date(yyyy-MM-dd) fully-qualified-path/to/output");
        }
    }
}
