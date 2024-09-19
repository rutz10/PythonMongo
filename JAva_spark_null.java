import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class NullCheckExample {
    public static void main(String[] args) {
        // Create a Spark session
        SparkSession spark = SparkSession.builder()
                .appName("Null Check Example")
                .master("local") // Change as needed
                .getOrCreate();

        // Sample data
        String[] data = {"1,Alice", "2,", "3,Charlie"};
        String schema = "id INT, name STRING";

        // Create DataFrame from sample data
        Dataset<Row> df = spark.read()
                .option("header", false)
                .schema(schema)
                .csv(spark.createDataset(java.util.Arrays.asList(data), Encoders.STRING()));

        // Check for nulls in the 'name' column
        long nullCount = df.filter(functions.col("name").isNull()).count();
        
        if (nullCount > 0) {
            throw new RuntimeException("Null values found in the 'name' column.");
        }

        // Continue processing if no nulls
        System.out.println("No null values found in the 'name' column.");

        // Stop the Spark session
        spark.stop();
    }
}
