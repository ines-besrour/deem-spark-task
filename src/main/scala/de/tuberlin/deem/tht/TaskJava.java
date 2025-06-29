package de.tuberlin.deem.tht;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.annotation.Target;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

        JavaPairRDD<String, String> reviewedTagged = reviewedProductIds
                .mapToPair(id -> new Tuple2<>(id, "REVIEWED"));

        JavaPairRDD<String, String> productsTagged = products
                .mapToPair(entry -> {
                    Tuple2<String, String> val = entry._2;
                    return new Tuple2<>(entry._1, "PRODUCT\t" + val._1 + "\t" + val._2);
                });

        JavaPairRDD<String, String> union = reviewedTagged.union(productsTagged);

        JavaPairRDD<String, Iterable<String>> grouped = union.groupByKey();

        JavaPairRDD<String, Tuple2<String, String>> productsWithoutReview = grouped.flatMapToPair(entry -> {
            boolean isReviewed = false;
            Tuple2<String, String> productData = null;

            for (String value : entry._2) {
                if (value.equals("REVIEWED")) {
                    isReviewed = true;
                } else if (value.startsWith("PRODUCT")) {
                    String[] parts = value.split("\t", 3);
                    if (parts.length == 3) {
                        productData = new Tuple2<>(parts[1], parts[2]);
                    }
                }
            }

            if (!isReviewed && productData != null) {
                return java.util.Collections.singletonList(new Tuple2<>(entry._1, productData)).iterator();
            } else {
                return java.util.Collections.emptyIterator();
            }
        });

        JavaRDD<Integer> kitchenWordCounts = productsWithoutReview
                .filter(product -> product._2._1.equals("Kitchen"))
                .map(product -> product._2._2.trim().split("\\s+").length);

        long kitchenProductCount = kitchenWordCounts.count();
        long totalWordCount = kitchenWordCounts.reduce(Integer::sum);

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            if (kitchenProductCount == 0) {
                String message = "No Kitchen products without reviews found.";
                System.out.println(message);
                writer.write(message);
                writer.newLine();
            } else {
                double avgWords = (double) totalWordCount / kitchenProductCount;
                String message = "Average number of words in description of Kitchen products with no reviews: " + avgWords;
                System.out.println(message);
                writer.write(message);
                writer.newLine();
            }
            writer.write("Time taken (ms): " + durationMs);
            System.out.println("Time taken (ms): " + durationMs);
        } catch (IOException e) {
            e.printStackTrace();
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
