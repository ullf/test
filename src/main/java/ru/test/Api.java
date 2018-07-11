package ru.test;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Api{

    public Dataset<Row> createCsvDataset(SparkSession spark,String filename,String tempview){
        Dataset<Row> df=spark.read().option("header",true).csv(filename);
        try {
            df.createGlobalTempView(tempview);
        } catch (AnalysisException e) {
            e.printStackTrace();
        }
        return df;
    }

    public void queryMin(String table,Dataset<Row> df){
        Dataset<Row> data=df.sqlContext().sql("SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp."+table+" GROUP BY dt");
        data.write().mode("append").format("parquet").save(table+".parquet");
    }

    public void queryMax(String table,Dataset<Row> df){
        Dataset<Row> data=df.sqlContext().sql("SELECT dt,max(AverageTemperature)  AS AverageTemperature FROM global_temp."+table+" GROUP BY dt");
        data.write().mode("append").format("parquet").save(table+".parquet");
    }

    public void queryAvg(String table,Dataset<Row> df){
        Dataset<Row> data=df.sqlContext().sql("SELECT dt,avg(AverageTemperature) AS AverageTemperature FROM global_temp."+table+" GROUP BY dt");
        data.write().mode("append").format("parquet").save(table+".parquet");
        data.write().mode("append").format("parquet").save(table+".parquet");
    }

    public void queryForPeriodOfTimeAvg(String table,Dataset<Row> df, String dt,int years){
        if(table.equals("global")){
            Dataset<Row> data=df.sqlContext().sql("SELECT sum(LandAverageTemperature)/(12*"+years+") AS LandAverageTemperature  FROM ( SELECT dt,avg(LandAverageTemperature) AS LandAverageTemperature FROM global_temp."+table+" WHERE dt BETWEEN '"+dt+"' AND '"+String.valueOf(Integer.parseInt(dt.split("-")[0])+years)+"-01-01' GROUP BY dt)");
            data.write().mode("append").format("parquet").save(table+".parquet");
        }else{
            Dataset<Row> data=df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*"+years+") AS AverageTemperature  FROM ( SELECT dt,avg(AverageTemperature) AS AverageTemperature FROM global_temp."+table+" WHERE dt BETWEEN '"+dt+"' AND '"+String.valueOf(Integer.parseInt(dt.split("-")[0])+years)+"-01-01' GROUP BY dt)");
            data.write().mode("append").format("parquet").save(table+".parquet");
        }

    }

    public void queryForPeriodOfTimeMax(String table,Dataset<Row> df, String dt,int years){
        if(table.equals("global")){
            Dataset<Row> data=df.sqlContext().sql("SELECT sum(LandAverageTemperature)/(12*"+years+") AS LandAverageTemperature  FROM ( SELECT dt,max(LandAverageTemperature) AS LandAverageTemperature FROM global_temp."+table+" WHERE dt BETWEEN '"+dt+"' AND '"+String.valueOf(Integer.parseInt(dt.split("-")[0])+years)+"-01-01' GROUP BY dt)");
            data.write().mode("append").format("parquet").save(table+".parquet");
        }
        else{
            Dataset<Row> data=df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*"+years+") AS AverageTemperature  FROM ( SELECT dt,max(AverageTemperature) AS AverageTemperature FROM global_temp."+table+" WHERE dt BETWEEN '"+dt+"' AND '"+String.valueOf(Integer.parseInt(dt.split("-")[0])+years)+"-01-01' GROUP BY dt)");
            data.write().mode("append").format("parquet").save(table+".parquet");
        }

    }

    public void queryForPeriodOfTimeMin(String table,Dataset<Row> df, String dt,int years){
        if(table.equals("global")){
            Dataset<Row> data=df.sqlContext().sql("SELECT sum(LandAverageTemperature)/(12*"+years+") AS LandAverageTemperature FROM ( SELECT dt,min(LandAverageTemperature) AS LandAverageTemperature FROM global_temp."+table+" WHERE dt BETWEEN '"+dt+"' AND '"+String.valueOf(Integer.parseInt(dt.split("-")[0])+years)+"-01-01' GROUP BY dt)");
            data.write().mode("append").format("parquet").save(table+".parquet");
        }else{
            Dataset<Row> data=df.sqlContext().sql("SELECT sum(AverageTemperature)/(12*"+years+") AS AverageTemperature FROM ( SELECT dt,min(AverageTemperature) AS AverageTemperature FROM global_temp."+table+" WHERE dt BETWEEN '"+dt+"' AND '"+String.valueOf(Integer.parseInt(dt.split("-")[0])+years)+"-01-01' GROUP BY dt)");
            data.write().mode("append").format("parquet").save(table+".parquet");
        }

    }
}
