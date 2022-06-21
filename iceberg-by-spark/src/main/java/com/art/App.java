package com.art;

import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {

        SparkSession spark = SparkSession
                .builder()
                .config("", "")
                .getOrCreate();

        spark.sql("");


        spark.stop();
    }
}
