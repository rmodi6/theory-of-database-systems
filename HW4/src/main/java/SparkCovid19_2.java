import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Map;
import java.util.Objects;

public class SparkCovid19_2 {

    public static void main(String[] args) {
        if (args.length == 3) {
            SparkConf conf = new SparkConf().setMaster("local").setAppName("SparkCovid19_2");
            JavaSparkContext sc = new JavaSparkContext(conf);

            Map<String, Long> locationToPopulationMap = sc.textFile(args[1])
                    .mapToPair(line -> {
                        try {
                            String[] split = line.split(",");
                            return new Tuple2<>(split[1], Long.parseLong(split[4]));
                        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                            System.out.println("Exception for line: " + line);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collectAsMap();
            Broadcast<Map<String, Long>> broadcastVariable = sc.broadcast(locationToPopulationMap);

            JavaRDD<String> textFile = sc.textFile(args[0]);
            JavaRDD<Tuple2<String, Double>> counts = textFile
                    .mapToPair(value -> {
                        try {
                            String[] split = value.split(",");
                            if (split.length == 4) {
                                String location = split[1];
                                int newCases = Integer.parseInt(split[2]);
                                return new Tuple2<>(location, newCases);
                            } else {
                                return null;
                            }
                        } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .reduceByKey(Integer::sum)
                    .map((Function<Tuple2<String, Integer>, Tuple2<String, Double>>) tuple2 -> {
                        String key = tuple2._1();
                        int value = tuple2._2();
                        if (broadcastVariable.getValue().containsKey(key)) {
                            double result = value * 1_000_000.0 / broadcastVariable.getValue().get(key);
                            return new Tuple2<>(key, result);
                        } else {
                            return null;
                        }
                    })
                    .filter(Objects::nonNull);

            counts.saveAsTextFile(args[2]);
        } else {
            System.out.println("Usage: spark-submit --class SparkCovid19_2 Covid19.jar fully-qualified-path/to/input fully-qualified-path/to/cache fully-qualified-path/to/output");
        }
    }
}
