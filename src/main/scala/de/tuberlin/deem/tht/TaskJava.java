package de.tuberlin.deem.tht;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.lang.annotation.Target;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class TaskJava {

    private static void withSpark(SparkSession session) {
        String productFile = "/app/data/products.tsv";
        String reviewFile = "/app/data/reviews.tsv";
        String outputFile = "/app/output/output.txt";

        long startTime = System.currentTimeMillis();

        JavaRDD<String> productLines = session.read().textFile(productFile).javaRDD();
        JavaPairRDD<String, Tuple2<String, String>> products = productLines
                .filter(line -> !line.startsWith("id"))
                .mapToPair(line -> {
                    String[] parts = line.split("\t");
                    return new Tuple2<>(parts[0], new Tuple2<>(parts[1], parts[2]));
                });

        JavaRDD<String> reviewLines = session.read().textFile(reviewFile).javaRDD();
        JavaRDD<String> reviewedProductIds = reviewLines
                .filter(line -> !line.startsWith("id"))
                .map(line -> line.split("\t")[0]);

        List<String> reviewIdList = reviewedProductIds.collect();
        Set<String> reviewSet = new HashSet<>(reviewIdList);
        Broadcast<Set<String>> broadcastReviewSet = session.sparkContext()
                .broadcast(reviewSet, scala.reflect.ClassTag$.MODULE$.apply(Set.class));

        JavaRDD<Integer> kitchenWordCounts = products
                .filter(product -> {
                    String productId = product._1;
                    String category = product._2._1;
                    return !broadcastReviewSet.value().contains(productId) && category.equals("Kitchen");
                })
                .map(product -> {
                    String description = product._2._2;
                    return description.trim().split("\\s+").length;
                });

        long kitchenProductCount = kitchenWordCounts.count();
        long totalWordCount = kitchenWordCounts.reduce(Integer::sum);

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;

        try (FileWriter writer = new FileWriter(outputFile)) {
            if (kitchenProductCount == 0) {
                writer.write("No Kitchen products without reviews found.\n");
                System.out.println("No Kitchen products without reviews found.");
            } else {
                double avgWords = (double) totalWordCount / kitchenProductCount;
                String result = "Average number of words in description of Kitchen products with no reviews: " + avgWords;
                writer.write(result + "\n");
                writer.write("Time taken (ms): " + durationMs + "\n");
                System.out.println(result);
                System.out.println("Time taken (ms): " + durationMs);
            }
        } catch (IOException e) {
            System.err.println("Error writing output: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        SparkSession session = null;

        handleIllegalReflectiveAccessSpark();

        try {
            session = SparkSession.builder()
                    .master("local")
                    .appName("tht")
                    .config("spark.some.config.option", "some-value")
                    .getOrCreate();

            session.sparkContext().setCheckpointDir(System.getProperty("java.io.tmpdir"));

            withSpark(session);

        } finally {
            if (session != null) {
                session.stop();
                System.clearProperty("spark.driver.port");
            }
        }
    }

    public static void handleIllegalReflectiveAccessSpark() {
        Module pf = org.apache.spark.unsafe.Platform.class.getModule();
        Target.class.getModule().addOpens("java.nio", pf);
        Target.class.getModule().addOpens("java.io", pf);

        Module se = org.apache.spark.util.SizeEstimator.class.getModule();
        Target.class.getModule().addOpens("java.util", se);
        Target.class.getModule().addOpens("java.net", se);
        Target.class.getModule().addOpens("java.lang", se);
        Target.class.getModule().addOpens("java.lang.ref", se);
        Target.class.getModule().addOpens("java.util.concurrent", se);
    }
}
