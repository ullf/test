package ru.test;

import org.apache.spark.sql.*;

public class App 
{
    public static void main( String[] args )
    {

        System.setProperty("hadoop.home.dir", "D:\\spark\\");
        Api api=new Api();

        SparkSession spark = SparkSession
                .builder()
                .appName("Spark Test application")
                .config("spark.master","local[*]")
                .getOrCreate();

        Dataset<Row> df=api.createCsvDataset(spark,"GlobalLandTemperaturesByCity.csv","city");
        Dataset<Row> df2=api.createCsvDataset(spark,"GlobalLandTemperaturesByCountry.csv","country");
        Dataset<Row> df3=api.createCsvDataset(spark,"GlobalTemperatures.csv","global");

        api.queryAvg("city",df);
       // df.sqlContext().sql("SELECT dt,avg(AverageTemperature) FROM global_temp.city GROUP BY dt").show();
        api.queryMin("city",df);
       // df.sqlContext().sql("SELECT dt,min(AverageTemperature) FROM global_temp.city GROUP BY dt").show();
        api.queryMax("city",df);
       // df.sqlContext().sql("SELECT dt,max(AverageTemperature) FROM global_temp.city GROUP BY dt").show();
        api.queryForPeriodOfTimeAvg("city",df,"1910-01-01",10);
        //df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*10) FROM ( SELECT dt,avg(AverageTemperature) AS AverageTemperature FROM global_temp.city WHERE dt BETWEEN '1910-01-01' AND '1920-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMin("city",df,"1910-01-01",10);
        //df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*10) AS MinimiumAvgTempr FROM ( SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp.city WHERE dt BETWEEN '1910-01-01' AND '1920-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMax("city",df,"1910-01-01",10);
        // df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*10) AS MaxAvgTempr FROM ( SELECT dt,max(AverageTemperature) AS AverageTemperature FROM global_temp.city WHERE dt BETWEEN '1910-01-01' AND '1920-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeAvg("city",df,"1910-01-01",89);
        //df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,avg(AverageTemperature) AS AverageTemperature FROM global_temp.city WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMin("city",df,"1910-01-01",89);
        //df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp.city WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMax("city",df,"1910-01-01",89);
        //df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,max(AverageTemperature) AS AverageTemperature FROM global_temp.city WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();

        api.queryMin("country",df2);
      //  df2.sqlContext().sql("SELECT dt,min(AverageTemperature) FROM global_temp.country GROUP BY dt").show();
        api.queryMax("country",df2);
        //df2.sqlContext().sql("SELECT dt,max(AverageTemperature) FROM global_temp.country GROUP BY dt").show();
        api.queryForPeriodOfTimeAvg("country",df2,"1910-01-01",89);
        //df2.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,avg(AverageTemperature) AS AverageTemperature FROM global_temp.country WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMin("country",df2,"1910-01-01",89);
        //df2.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp.country WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMax("country",df2,"1910-01-01",89);
        //df2.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,max(AverageTemperature) AS AverageTemperature FROM global_temp.country WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();

        api.queryAvg("global",df3);
        //df3.sqlContext().sql("SELECT dt,avg(AverageTemperature) FROM global_temp.global GROUP BY dt").show();
        api.queryMin("global",df3);
        //df3.sqlContext().sql("SELECT dt,min(AverageTemperature) FROM global_temp.global GROUP BY dt").show();
        api.queryMax("global",df3);
        //df3.sqlContext().sql("SELECT dt,max(AverageTemperature) FROM global_temp.global GROUP BY dt").show();
        api.queryForPeriodOfTimeMin("global",df3,"1910-01-01",10);
        //df3.sqlContext().sql("SELECT sum(AverageTemperature)/(12*10) AS MinimiumAvgTempr FROM ( SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp.global WHERE dt BETWEEN '1910-01-01' AND '1920-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMax("global",df3,"1910-01-01",10);
        //df3.sqlContext().sql("SELECT sum(AverageTemperature)/(12*10) AS MinimiumAvgTempr FROM ( SELECT dt,max(AverageTemperature) AS AverageTemperature FROM global_temp.global WHERE dt BETWEEN '1910-01-01' AND '1920-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeAvg("global",df3,"1910-01-01",10);
        //df3.sqlContext().sql("SELECT sum(AverageTemperature)/(12*10) AS MinimiumAvgTempr FROM ( SELECT dt,avg(AverageTemperature) AS AverageTemperature FROM global_temp.global WHERE dt BETWEEN '1910-01-01' AND '1920-01-01' GROUP BY dt)").show();

        api.queryForPeriodOfTimeMin("global",df3,"1910-01-01",89);
        //df3.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp.global WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();
        api.queryForPeriodOfTimeMax("global",df3,"1910-01-01",89);
        //df2.sqlContext().sql("SELECT sum(AverageTemperature)/(12*89) FROM ( SELECT dt,max(AverageTemperature) AS AverageTemperature FROM global_temp.global WHERE dt BETWEEN '1910-01-01' AND '1999-01-01' GROUP BY dt)").show();

        if(spark!=null){
            spark.close();
        }

    }

}
