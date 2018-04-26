package com.sc.eni.main

import com.sc.eni.sparktohbase.HbaseProvider
import com.sc.eni.sparktohbase.HbaseWithSpark._
import com.sc.eni.transformation.transform._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

object RunApp extends HbaseProvider {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    // directories for L1, L2 files.
    val inputfilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l1/csv/emp1.csv"
    val avrofilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l2/avro/emp1.avro"
    val parquetfilepath = "hdfs://quickstart.cloudera:8020/tssdemo2/l2/parquet/emp1.parquet"

    //Reading RawFile from L1 into RDD
    val filerdd : RDD[String] = sc.textFile(inputfilepath)
    println("LOG_INFO : Printing the input RAW csv file As RDD ..... ")
    println(filerdd.foreach(println))

    /*transformation logic */
    val emprdd2 = transformlogic(filerdd)

    /* Convert to Dataframe */
    import sqlContext.implicits._
    val df:DataFrame  = emprdd2.toDF("empno","empname","emploc","empscore","empoldsalary","empnewsalary")
    println("LOG_INFO : Printing the transformed data as a DATAFRAME ..... ")
    df.show()

    //convert to avro file and save to l2 at hdfs
    writetoavro(df,avrofilepath)

    //Read avro from l2 and convert to df
    readfromavro(sqlContext,avrofilepath)

    //Convert Df to parquet file at L3 on HDFS
    writetoparquet(df, parquetfilepath)

    //convert paquet file to Df to check the data
    readfromparquet(sqlContext, parquetfilepath)

    //Reading Parquet file from L2 and Loading it into Hbase at L3

    val tableName = "bulkload"
    val columnFamily1 = "personal"
    val columnFamily2 = "prof"

    val inputrdd: RDD[Row] = df.rdd
    val stringrdd: RDD[String] = inputrdd.map(line => line.toString())
    val toRemove = "[]".toSet
    val correctedrdd: RDD[String] = stringrdd.map(lines => lines.filterNot(toRemove))
    val splitrdd: RDD[Array[String]] = correctedrdd.map(line => line.split(","))

    // Input Data : in RDD with two cf1 and two rows :
    // (RowKey, columnFamily, columnQualifier, value)
    val rdd123: RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = splitrdd.map(
      lines =>rowtocolumn(lines,columnFamily1, columnFamily2)
    ).flatMap(x=> x)

    //If table exists delete it
    deletetableifexists(tableName)

    //create table
    createtable(tableName, columnFamily1, columnFamily2)

    // BUlkPut the rdd into HBase table "tableName' -
    println("putting the value into hbase ************")
    bulkputcolumns(hbaseContext,rdd123,tableName)

  }



}
