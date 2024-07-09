package de.tuberlin.deem.tht

import org.apache.spark.sql.SparkSession

object Task extends App {

  withSpark { session =>

    // IMPLEMENT SOLUTION HERE

  }

  def withSpark(func: SparkSession => Unit): Unit = {

    val session = SparkSession.builder()
      .master("local")
      .appName("tht")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", 2.toString)
      .getOrCreate()
    session.sparkContext.setCheckpointDir(System.getProperty("java.io.tmpdir"))

    try {
      func(session)
    } finally {
      session.stop()
      System.clearProperty("spark.driver.port")
    }
  }

}