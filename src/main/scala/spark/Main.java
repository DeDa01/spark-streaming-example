package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import spark.exampleSource.read.stream.DefaultSource;

import java.util.concurrent.TimeoutException;

public class Main {


    public static void main(String[] args) {

        SparkSession spark = SparkSession
                .builder()
                .appName("spark-streaming-example")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> stream = spark
                .readStream()
                .format(DefaultSource.class.getPackageName())
                .option("minOffset", System.currentTimeMillis() / 1000)
                .load();

        try {
            stream
                    .writeStream()
                    .outputMode("append")
                    .format("console")
                    .start()
                    .awaitTermination();
        } catch (StreamingQueryException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }



}
