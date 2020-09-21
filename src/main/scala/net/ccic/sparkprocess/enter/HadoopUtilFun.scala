package net.ccic.sparkprocess.enter

import org.apache.hadoop.fs.Path

/**
  * Created by FrankSen on 2018/7/2.
  */
trait HadoopUtilFun {
  /**
    * 检查指定文件是否存在
    */
  def checkExists(filePath: String): Boolean
  def checkExists(filePath: Path): Boolean
  /**
    * 创建HDFS路径
    * */
  def createDir(path :String):Boolean
  def createDir(path :Path):Boolean

}
