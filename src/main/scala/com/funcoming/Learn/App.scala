package com.funcoming.Learn

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.spark.{HBaseContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
//这个很关键，能不能用，就看它了。。
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._

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
      (Bytes.toBytes("rowkeyForeachpartion1"),
        Array((Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue1")))),
      (Bytes.toBytes("rowkeyForeachpartion2"),
        Array((Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue2")))),
      (Bytes.toBytes("rowkeyForeachpartion3"),
        Array((Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue3")))),
      (Bytes.toBytes("rowkeyForeachpartion4"),
        Array((Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue4")))),
      (Bytes.toBytes("rowkeyForeachpartion5"),
        Array((Bytes.toBytes("columnFamilymylittleFamily"), Bytes.toBytes("column1"), Bytes.toBytes("columnValue5"))))
    ))
    //这是 HBASE 的 bulkPut方法。。
    //    hBaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd, TableName.valueOf("myLittleHbaseTable"),
    //      (putRecord) => {
    //        val put1 = new Put(putRecord._1)
    //        putRecord._2.foreach((putValue) => {
    //          put1.addColumn(putValue._1, putValue._2, putValue._3)
    //        }) //foreach
    //        put1
    //      })
    //这是 HBASE 的 hbaseforeachpartition方法
    rdd.hbaseForeachPartition(hBaseContext, (it, con) => {
      //  还真是connection
      val bufferedMutator = con.getBufferedMutator(TableName.valueOf("myLittleHbaseTable"))
      it.foreach(r => {
        //        用 rowkey 来实例化..
        val put1 = new Put(r._1)
        //        看来要分别搞列族，列，和值。。
        r._2.foreach((putValue) => {
          put1.addColumn(putValue._1, putValue._2, putValue._3)
        })
        bufferedMutator.mutate(put1)
      }) //it.foreach finished
      bufferedMutator.flush()
      bufferedMutator.close()
    })

    try {

    } catch {
      case ex: Exception => {

      }
    } finally {

    }


  }

}
