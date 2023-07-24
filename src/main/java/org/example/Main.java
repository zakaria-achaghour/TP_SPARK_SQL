package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {
    public static void main(String[] args) {

        // Create a SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Incident")
                .master("local[*]") // Run Spark locally using all available cores
                .getOrCreate();
        // Load the CSV file into a DataFrame
        Dataset<Row> incidentsDF = spark.read()
                .option("header", "true")
                .csv("incidents.csv");
        // Register the DataFrame as a temporary view to perform SQL queries
        incidentsDF.createOrReplaceTempView("incidents");
        // Perform SQL query to get the number of incidents per service
        Dataset<Row> incidentsPerServiceDF = spark.sql("SELECT service, COUNT(*) AS num_incidents FROM incidents GROUP BY service");
        // Show the results
        incidentsPerServiceDF.show();


        // Extract the year from the date column and create a new column "year"
        Dataset<Row> incidentsWithYearDF = spark.sql(
                "SELECT *, YEAR(date) AS year FROM incidents");

        incidentsWithYearDF.createOrReplaceTempView("incidents_year");

        Dataset<Row> incidentsPerYearDF = spark.sql(
                "SELECT year, COUNT(*) AS num_incidents FROM incidents_year GROUP BY year");

        incidentsPerYearDF = incidentsPerYearDF.orderBy(org.apache.spark.sql.functions.col("num_incidents").desc());

        incidentsPerYearDF.show(2);
        // Stop the SparkSession
        spark.stop();


    }

}