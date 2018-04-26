package com.sc.eni.transformation

import java.io.File

import com.databricks.spark.avro._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


object transform extends SparkProvider  {

  def transformlogic(filerdd:RDD[String]) : RDD[(String,String,String,Int,Float,Float)] ={
    val emprdd = filerdd.map {
      line =>
        val col = line.split(",")
        (col(0), col(1), col(2), col(3).toInt, col(4).toFloat)
    }.map{
      case(empno,empname,emploc,empscore, empsalary) =>
        if(empscore >= 85) (empno, empname, emploc, empscore, empsalary, "%.2f".format(empsalary*(1.25)).toFloat)
        else if(empscore >=70) (empno, empname, emploc, empscore, empsalary, "%.2f".format(empsalary*(1.20)).toFloat)
        else if(empscore >=55) (empno, empname, emploc, empscore, empsalary, "%.2f".format(empsalary*(1.15)).toFloat)
        else  (empno, empname, emploc, empscore, empsalary,  "%.2f".format(empsalary*(1.05)).toFloat)
    }
    println("\nLOG_INFO : Printing the Transformed RDD ..... ")
    println(emprdd.foreach(println))
    emprdd
  }

  def writetoavro (df: DataFrame, filepath:String): Unit ={

    val f = new File(filepath)
    df.write.mode(SaveMode.Overwrite).avro(filepath)
    println("\nLOG_INFO : Transformed Data is written in AVRO file format at Dir : "+filepath+" ..... ")
  }

  def readfromavro(sqlContext: SQLContext, filepath:String): Unit ={
    val dfavro = sqlContext.read
      .format("com.databricks.spark.avro")
        .load(filepath)
    println("\nLOG_INFO : CHECKING TRANSFORMED AVRO FILE ..... \nLOG_INFO : Reading Avro file at path : "+filepath+" ..... ")
    dfavro.show()
  }

  def writetoparquet(frame: DataFrame, filepath:String): Unit ={
    frame.write.parquet(filepath)
    println("\nLOG_INFO : Transformed Data is written in PARQUET file format at Dir : "+filepath+" ..... ")
  }

  def readfromparquet(context: SQLContext, filepath:String): Unit ={
    val dffromparquet: DataFrame = context.read.parquet(filepath)
    println("\nLOG_INFO : CHECKING TRANSFORMED Parquet FILE ..... \nLOG_INFO : Reading Parquet file at path : "+filepath+" ..... ")
    dffromparquet.show()
  }


}
