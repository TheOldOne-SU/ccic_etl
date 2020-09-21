package net.ccic.sparkprocess.enter.impl

import java.text.SimpleDateFormat
import java.util.Date

import net.ccic.sparkprocess.enter.HadoopUtilFun
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by FrankSen on 2018/7/2.
  */
object HadoopUtilImpl extends HadoopUtilFun{

  val conf = new Configuration()

  val hdfs = FileSystem.get(conf)




  /**
    * 创建HDFS路径
    **/
  override def createDir(path: String): Boolean = ???

  override def createDir(path: Path): Boolean = ???

  override def checkExists(filePath: Path): Boolean= {
    printMessage("开始检查指定文件是否存在：" + filePath.getName)
    val fs = filePath.getFileSystem(conf)
    fs.exists(filePath)
  }

  override def checkExists(filePath: String): Boolean = {
    printMessage("开始检查指定文件是否存在：" + filePath)
    val path = new Path(filePath)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }

  private def printMessage(msg: String) {
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date) + " | " + msg)
  }

}
