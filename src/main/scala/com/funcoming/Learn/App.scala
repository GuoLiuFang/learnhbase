package com.funcoming.Learn

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.{Bytes, Writables}

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
    try {

    } catch {
      case ex: Exception => {

      }
    } finally {

    }


  }

}
