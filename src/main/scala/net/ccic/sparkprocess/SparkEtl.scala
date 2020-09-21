package net.ccic.sparkprocess

import net.ccic.sparkprocess.service.impl.SparkEtlModelServiceImpl

/**
  * Created by FrankSen on 2018/7/24.
  */
class SparkEtl(args: Array[String]) {

  assert(args.length <= 6,s"Parameter over limit 5, have ${args.length}.")

  def executeSparkEtlService(): Unit = {
    SparkEtlModelServiceImpl.sparkEtlModelService(args)
  }
}

/**
  * SparkSql enter
  * param list:
  * arg(0) -> master.url
  * arg(1) -> mode
  * arg(2) -> sql
  * arg(3) -> tbl_name
  */
object SparkEtl{
  def main(args: Array[String]): Unit = {
    assert(args.length > 0, "Not enough arguments: missing some values")
    val sparkEtlCli = new SparkEtl(args)
    sparkEtlCli.executeSparkEtlService()
  }
}
