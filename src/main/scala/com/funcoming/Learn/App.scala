package com.funcoming.Learn

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.{HBaseContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

/**
  * @author ${user.name}
  */
object App {

  def foo(x: Array[String]) = x.foldLeft("")((a, b) => a + b)

  def main(args: Array[String]) {
    println("Hello World!")
    println("concat arguments = " + foo(args))
    val configuration = HBaseConfiguration.create()
    val connection = ConnectionFactory.createConnection(configuration)
    //    第二步就是：获取管理员权限
    //    val admin = connection.getAdmin
    //    val tabledesc = new HTableDescriptor(TableName.valueOf("myLittleHbaseTable"))
    //    val columnDescriptor = new HColumnDescriptor(Bytes.toBytes("columnFamilymylittleFamily"))
    //    tabledesc.addFamily(columnDescriptor)
    //    admin.createTable(tabledesc)
    //    val listtables = admin.listTables()
    //    val listtables = admin.listTableNames()
    //    listtables.foreach(println)
    val table = connection.getTable(TableName.valueOf("myLittleHbaseTable"))

    val put = new Put(Bytes.toBytes("rowkey1mylittleRow"))
    put.addColumn(Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("quanlifier--lie"), Bytes.toBytes("valuezhi"))
    table.put(put)
    //------------完成了数据的插入。。
    //    正好测试下，相同数据能不能插入的问题
    val get = new Get(Bytes.toBytes("rowkey1mylittleRow"))
    val result = table.get(get)
    val resultBytes = result.getValue(Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("quanlifier--lie"))
    println("rowkey1mylittleRow--->columnFamilymylittleFamily--->quanlifier的名字为" + Bytes.toString(resultBytes))
    val scan = new Scan()
    //    还要 Adorn it with column names...
    scan.addColumn(Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("quanlifier--lie"))
    val resultScanner = table.getScanner(scan)
    val iteratorScan = resultScanner.iterator()
    while (iteratorScan.hasNext) {
      println("用scan 获取到的结果", iteratorScan.next)

    }
    val sparkContext = new SparkContext()
    //    val hBaseConfiguration = HBaseConfiguration.create()
    val hBaseContext = new HBaseContext(sparkContext, configuration)
    val rdd = sparkContext.parallelize(Array(
      (Bytes.toBytes("rowkey1"),
        Array((Bytes.toBytes("columnFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue1")))),
      (Bytes.toBytes("rowkey2"),
        Array((Bytes.toBytes("columnFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue2")))),
      (Bytes.toBytes("rowkey3"),
        Array((Bytes.toBytes("columnFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue3")))),
      (Bytes.toBytes("rowkey4"),
        Array((Bytes.toBytes("columnFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue4")))),
      (Bytes.toBytes("rowkey5"),
        Array((Bytes.toBytes("columnFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue5"))))
    ))

    hBaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd, TableName.valueOf("myLittleHbaseTable"),
      (putRecord) => {
        val put1 = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => {
          put1.addColumn(putValue._1, putValue._2, putValue._3)
        }) //foreach
        put1
      })


    try {

    } catch {
      case ex: Exception => {

      }
    } finally {

    }


  }

}
