# deem-spark-task

The code loads product and review data, filters out reviewed products, and calculates the average word count of “Kitchen” product descriptions without reviews. It then writes the result and runtime to an output file.

## Requirements
- JDK 8 or later
- Docker
- Maven

## Steps to Run

1. **Pull the Spark Docker image**  
   Pull the latest Bitnami Spark image from Docker Hub:

   ```bash
   docker pull bitnami/spark:latest
   ```
2. **Build the Maven project**  
   Run the following command to package the Java application:

   ```bash
   mvn clean package
   ```

3. Run the Spark job using Docker
   Use the following command to execute the Spark job:
   ```bash
   docker run --rm -it \
    -v "${PWD}/target:/app/jar" \
    -v "${PWD}/data:/app/data" \
    -v "${PWD}/output:/app/output" \
    bitnami/spark:latest spark-submit \
    --conf "spark.driver.extraJavaOptions=--add-opens=java.base/java.lang.ref=ALL-UNNAMED" \
    --conf "spark.executor.extraJavaOptions=--add-opens=java.base/java.lang.ref=ALL-UNNAMED" \
    --class de.tuberlin.deem.tht.TaskJava \
    --master local[*] /app/jar/takehometest-1.0-SNAPSHOT.jar
   ```
