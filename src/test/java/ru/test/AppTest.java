package ru.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.*;

public class AppTest {

    private SparkSession spark;

    @BeforeClass
    public void startSparkBefore(){
        System.setProperty("hadoop.home.dir", "%SPARK_HOME%");
        spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.master","local")
                .getOrCreate();
    }

    @Test
    public void createDatasetTest(){
        Api api=new Api();
        Dataset<Row> df= api.createCsvDataset(spark,"GlobalLandTemperaturesByCity.csv","city");
    }

    @Test
    public void queryTest(){
        Api api=new Api();
        Dataset<Row> df= api.createCsvDataset(spark,"GlobalLandTemperaturesByCity.csv","city");
        df.sqlContext().sql("SELECT dt,avg(AverageTemperature) FROM global_temp.city GROUP BY dt");
    }

    @AfterClass
    public void closeAfter(){
        if(spark!=null){
            spark.close();
        }
    }
}
