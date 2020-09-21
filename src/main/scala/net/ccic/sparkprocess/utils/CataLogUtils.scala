package net.ccic.sparkprocess.utils

import java.io.FileInputStream
import java.net.URI
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier

/**
  * Created by FrankSen on 2019/4/29.
  */
object CataLogUtils {

  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")


  /**
    * stdout log generate
    * @param message message
    */
  def printMessage(message: String): Unit ={
    println(sdf.format(new Date) + " | " + message)
  }

  /**
    * 检查指定文件是否存在
    */
  def checkExists(filePath: String): Boolean = {
    printMessage("开始检查指定文件是否存在：" + filePath)
    val conf = new Configuration()
    val checkPath = new Path(filePath)
    val fs = checkPath.getFileSystem(conf)
    fs.exists(checkPath)
  }

  def generateDir(tableName: String): String ={
    val value_str = tableName.split("\\.")
    value_str(0).toLowerCase + ".db/" + value_str(1).toLowerCase
  }


  def loadProperties(cfgPath: String): Properties = {
    val p = new Properties()
    val appStream = new FileInputStream(cfgPath)
    p.load(appStream)
    p
  }

  private[ccic] def getTableFileNum(spark: SparkSession, outputTableName: String): TableFileStat ={
    val table_message = outputTableName.split("\\.")
    val u_table_path = spark.sessionState.catalog
      .getTableMetadata(TableIdentifier(table_message(1), Option(table_message(0))))
      .location.getPath
    val fs = FileSystem.get(new URI(u_table_path), new Configuration())
    val files = fs.listStatus(new Path(u_table_path))

    printMessage("table hdfs directory:" + u_table_path)
    var originFileNum = -1
    var length = 0l
    for(f <- files){
      length = f.getLen + length
      originFileNum += 1
    }

    printMessage(s"totalSize of table:$length")

    TableFileStat(originFileNum, length)
  }

}

/**
  *
  * @param originFileNum file number of the table directory on dfs
  * @param totalSize file totalSize
  */
case  class TableFileStat(originFileNum: Int, totalSize:Long)
