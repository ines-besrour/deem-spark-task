package de.tuberlin.deem.tht;

import org.apache.spark.sql.SparkSession;

public class TaskJava {

    private static void withSpark(SparkSession session) {

        // IMPLEMENT SOLUTION HERE

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
            session.stop();
            System.clearProperty("spark.driver.port");
        }
    }

    public static void handleIllegalReflectiveAccessSpark(){
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
