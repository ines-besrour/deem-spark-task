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
##  Spark Execution Steps
The code performs the following steps using Apache Spark:

- Load products.tsv and reviews.tsv into RDDs.
- Filter out header lines from both datasets.
- Parse products into (productId, (category, description)) pairs.
- Parse reviewed product IDs from the reviews file.
- Tag each review ID as "REVIEWED" and each product with "PRODUCT\tcategory\tdescription".
- Union both tagged RDDs into one dataset.
- Group all entries by productId using groupByKey (causing a shuffle).
- For each group, filter to keep only products without a "REVIEWED" tag.
- Filter remaining entries to only those with "Kitchen" category.
- Map each description to its word count.
- Count the number of matching products and reduce to sum total words.
- Compute the average word count and write results to output.txt.
