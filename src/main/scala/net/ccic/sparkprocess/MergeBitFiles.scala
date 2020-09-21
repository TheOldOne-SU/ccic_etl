package net.ccic.sparkprocess

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}
import util.control.Breaks.{breakable, break}

/**
  * 小文件合并
  */
object MergeBitFiles {

  /**
    *
    * @param args 源库 表名 覆盖(true/false) 目标库/分区键 分区值
    *             覆盖为true:
    *               如果全表所有分区都需要分别进行合并，不需指定参数4、5
    *               如果仅需合并指定分区，需指定参数：分区键、分区值
    *             覆盖为false:
    *               需指定参数: 目标库，不需指定参数5
    */
  def main(args: Array[String]): Unit = {

    if (args.length < 3) {
      System.exit(1)
    }

    val db = args(0)
    val tbName = args(1)
    val overWrite = args(2)
    var storePath = ""
    if (overWrite.toLowerCase.equals("false")) {
      if (args.length == 4) {
        storePath = args(3)
      } else {
        System.exit(2)
      }

    } else if (overWrite.toLowerCase.equals("true")) {
      println("+++++WARNING: overwrite current data!")

      //参数为5个，进入分区合并函数
      if (args.length == 5) {
        val partitionKey = args(3)
        val partitionVal = args(4)
        mergePartition(db, tbName, partitionKey, partitionVal)
        return
      }

      storePath = db
    } else {
      System.exit(3)
    }

    println(db, tbName, overWrite, storePath)
    merge(db, tbName, overWrite, storePath)
  }


  /**
    * 合并全表文件
    * @param db 源库
    * @param tbName 表名
    * @param overWrite 是否覆盖（true：是,false 否）
    * @param storeDB 如果overwrite为true不需指定,如果为false 需指定移动至的库名
    */
  def merge(db: String, tbName: String, overWrite: String, storeDB: String): Unit = {
    println("=====merge table bit files:" + db + "." + tbName)

    val baseTemp = "/tmp/MegerBitFiles/" + db + "/" + tbName + "/"
    println("=====base temp :" + baseTemp)

    val appName = "MegerBitFiles-" + db + "." + tbName + "-" + storeDB
    val conf = new SparkConf().setAppName(appName)
    val spark = SparkSession.builder().config(conf).config("spark.some.config.option", "some-value").getOrCreate()

    val header = "hdfs://nameservice1:8020"

    val basePath = "hdfs://nameservice1:8020/user/hive/warehouse/"
    val processPath = basePath + db + ".db/" + tbName

    val baseHdfsPath = processPath.replace(header, "")
    println("=====base dir:" + baseHdfsPath)
    val baseSavePath = processPath.replace(header, "").replace(db + ".db", storeDB + ".db")
    println("=====base save dir:" + baseSavePath)

    val blockSize = 125829120l

    val fs = FileSystem.get(new URI(processPath), new org.apache.hadoop.conf.Configuration())
    val dirs = fs.listStatus(new Path(baseHdfsPath))

    if (fs.exists(new Path(baseTemp))) {
      println("+++++base temp exists ,delete:" + baseTemp)
      fs.delete(new Path(baseTemp), true)
    }
    println("+++++create temp:" + baseTemp)
    fs.mkdirs(new Path(baseTemp))

    //    if (!fs.exists(new Path(baseSavePath))) {
    //      println("+++++base save dir not exists ,create :" + baseSavePath)
    //      fs.mkdirs(new Path(baseSavePath))
    //    }

    var hdfsPath = ""
    var tempPath = ""
    var originFileNum = 0
    var currentFileNum = 0
    var partNum = 0

    //遍历表下都分区
    for (d <- dirs) {
      hdfsPath = d.getPath.toString.replace(header, "")
      println("=====processing dir:" + hdfsPath)
      tempPath = baseTemp + hdfsPath.substring(hdfsPath.lastIndexOf("/") + 1)
      println("=====temp dir:" + tempPath)


      //遍历分区下都文件
      val files = fs.listStatus(d.getPath)
      var length = 0l
      //统计分区文件大小
      for (f <- files) {
        length = f.getLen + length
        originFileNum += 1
      }
      println("=====size:" + length)

      breakable {
        //分区文件大小为0，不进行处理
        if (length == 0) {
          break
        }

        //计算文件数
        partNum += 1
        var fn = 1
        if (length > blockSize) {
          fn = (length / blockSize + 1).intValue()
        }
        currentFileNum += fn
        println("=====file number:" + fn)

        //重分布文件
        val df = spark.read.parquet(hdfsPath)
        df.repartition(fn).write.format("parquet").mode(SaveMode.Overwrite).save(tempPath)
        //        df.coalesce(fn).write.format("parquet").mode(SaveMode.Overwrite).save(tempPath)
      }
    }
    println("=====merge finished")

    println("=====moving data")
    //覆盖源表文件
    if (overWrite.toLowerCase.equals("true")) {
      if (fs.exists(new Path(baseHdfsPath))) {
        println("+++++deleting... :" + baseHdfsPath)
        //        fs.delete(new Path(baseHdfsPath), true);
        fs.rename(new Path(baseHdfsPath), new Path(baseHdfsPath + ".bk"))
        println("+++++delete finished:" + baseHdfsPath)
      }

      println("+++++moving... :" + baseTemp)
      fs.rename(new Path(baseTemp), new Path(baseHdfsPath))
      println("+++++move finished:" + baseTemp)

      //合并到新到路径
    } else if (overWrite.toLowerCase.equals("false")) {
      println("=====move data to:" + baseSavePath)
      if (fs.exists(new Path(baseSavePath))) {
        println("=====file exists ,delete:" + baseSavePath)
        fs.rename(new Path(baseSavePath), new Path(baseSavePath + ".bk"))
      }
      fs.rename(new Path(baseTemp), new Path(baseSavePath))
    }

    //      val df = spark.read.parquet("/user/hive/warehouse/ccic_edw.db/dm_dash_main_channel_f")
    //      df.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).partitionBy("reportdate").save("/user/hive/warehouse/ccic_kylin.db/dm_dash_main_channel_f")
    println("=====partition number:" + partNum)
    println("=====origin file number:" + originFileNum)
    println("=====current file number:" + currentFileNum)
    println("=====process finished")
  }


  /**
    * 合并分区文件
    * @param db 库名
    * @param tbName 表名
    * @param partitionKey 分区键
    * @param partitionVal 分区值
    */
  def mergePartition(db: String, tbName: String, partitionKey: String, partitionVal: String): Unit = {
    println("=====merge partition bit files:" + db + "." + tbName + "." + partitionKey + "=" + partitionVal)

    val baseTemp = "/tmp/MegerBitFiles/" + db + "/" + tbName + "/" + partitionKey + "=" + partitionVal
    println("=====base temp :" + baseTemp)

    val appName = "MegerBitFiles-" + db + "." + tbName + "." + partitionKey + "=" + partitionVal
    val conf = new SparkConf().setAppName(appName)
    val spark = SparkSession.builder().config(conf).config("spark.some.config.option", "some-value").getOrCreate()

    val header = "hdfs://nameservice1:8020"

    val basePath = "hdfs://nameservice1:8020/user/hive/warehouse/"
    val processPath = basePath + db + ".db/" + tbName + "/" + partitionKey + "=" + partitionVal

    val baseHdfsPath = processPath.replace(header, "")
    println("=====base dir:" + baseHdfsPath)

    val blockSize = 125829120l

    val fs = FileSystem.get(new URI(processPath), new org.apache.hadoop.conf.Configuration())

    if (fs.exists(new Path(baseTemp))) {
      println("+++++base temp exists ,delete:" + baseTemp)
      fs.delete(new Path(baseTemp), true)
    }
    println("+++++create temp:" + baseTemp)
    fs.mkdirs(new Path(baseTemp))

    var originFileNum = -1

    val files = fs.listStatus(new Path(baseHdfsPath))
    var length = 0l
    for (f <- files) {
      length = f.getLen + length
      originFileNum += 1
    }

    println("=====size:" + length)


    var currentFileNum = 1
    if (length > blockSize) {
      currentFileNum = (length / blockSize + 1).intValue()
    }
    println("=====file number:" + currentFileNum)

    val df = spark.read.parquet(baseHdfsPath)
    df.repartition(currentFileNum).write.format("parquet").mode(SaveMode.Overwrite).save(baseTemp)

    println("=====merge finished")

    println("=====moving data")
    if (fs.exists(new Path(baseHdfsPath))) {
      println("+++++deleting... :" + baseHdfsPath)
      //        fs.delete(new Path(baseHdfsPath), true);
      fs.rename(new Path(baseHdfsPath), new Path(baseHdfsPath + ".bk"))
      println("+++++delete finished:" + baseHdfsPath)
    }
    println("+++++moving... :" + baseTemp)
    fs.rename(new Path(baseTemp), new Path(baseHdfsPath))
    println("+++++move finished:" + baseTemp)

    println("=====origin file number:" + originFileNum)
    println("=====current file number:" + currentFileNum)
    println("=====process finished")
  }

}
