package com.sc.eni.sparktohbase

import com.cloudera.spark.hbase.HBaseContext
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.spark.rdd.RDD


object HbaseWithSpark extends HbaseProvider {


  def deletetableifexists ( tableName :String ): Unit ={

    if(ad.tableExists(TableName.valueOf(tableName))){
      if(!ad.isTableDisabled(TableName.valueOf(tableName))){
        ad.disableTable(TableName.valueOf(tableName))
      }
      ad.deleteTable(TableName.valueOf(tableName))
    }

  }

  def createtable(tableName :String,columnFamily1: String, columnFamily2: String): Unit = {
    val tdescriptor: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

    // Adding column families to table descriptor
    tdescriptor.addFamily(new HColumnDescriptor(columnFamily1))
    tdescriptor.addFamily(new HColumnDescriptor(columnFamily2))

    // Execute the table through admin
    ad.createTable(tdescriptor)
  }

  //Converting RDD[Row] into RDD[columns] of type: (column of Hbase)
  def rowtocolumn(arrays: Array[String], columnFamily1 :String , columnFamily2: String): Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = {
    val result: Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = Array(
      (Bytes.toBytes(arrays(0)), Array(
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("empid"), Bytes.toBytes(arrays(0))),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("empname"), Bytes.toBytes(arrays(1))),
        (Bytes.toBytes(columnFamily1), Bytes.toBytes("emploc"), Bytes.toBytes(arrays(2))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("empscore"), Bytes.toBytes(arrays(3))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("empoldsal"), Bytes.toBytes(arrays(4))),
        (Bytes.toBytes(columnFamily2), Bytes.toBytes("empnewsal"), Bytes.toBytes(arrays(5)))
      )))
    val fin =result
    fin
  }

  def bulkputcolumns(hBaseContext: HBaseContext, rdd:RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])], tableName: String): Unit = {
println{"entered bulkputcolumns *************"}
    hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](
      rdd,
      tableName,
      (putRecord) => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      },
      "true".toBoolean)
    println{"exiting bulkputcolumns *************"}
  }


}
