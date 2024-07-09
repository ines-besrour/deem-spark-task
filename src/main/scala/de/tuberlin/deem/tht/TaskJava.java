package de.tuberlin.deem.tht;

import org.apache.spark.sql.SparkSession;

public class TaskJava {

    private static void withSpark(SparkSession session) {

        // IMPLEMENT SOLUTION HERE

    }

    public static void main(String[] args) {

        SparkSession session = null;

        try {
            session = SparkSession.builder()
                .master("local")
                .appName("tht")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

            session.sparkContext().setCheckpointDir(System.getProperty("java.io.tmpdir"));

            withSpark(session);

        } finally {
            session.stop();
            System.clearProperty("spark.driver.port");
        }
    }
}
