package net.ccic.sparkprocess.enter

import org.apache.spark.sql.SparkSession

/**
  * Created by FrankSen on 2018/7/2.
  */
trait SparkEtlFun {

  /**
    *
    * @param sql 插入分区类型SQL语句 如：
    *            alter table ccic_dev.test_partition_prpcmain_new
    *              add if not exists partition(workdate=20180620);
    *            insert overwrite table ccic_dev.test_partition_prpcmain_new
    *               partition(workdate=20180620)
    *               select
    *               policyno
    *               , startdate
    *               , enddate
    *               from ccic_source.t03_prpcmain
    */
  def insertPartition(sql: String, spark: SparkSession)

  /**
    * 功能：备份指定parquet文件
    * 参数：pathDir 读取和存储目录
    *     tableName 需要备份的表名
    *     [bkupTableName 指定备份表名,默认为"${tableName}.tmp"]
    */
  def backupParquet(master_ulr: String, spark: SparkSession, pathDir: String, tableName: String, bkupTableName: String)


  /**
    * 功能：删除表
    * 参数：pathDir 读取或存储hdfs位置
    * 			tableName 指定表名
    */
  def removeParquet(master_ulr: String, spark: SparkSession, pathDir: String, tableName: String)

  /**
    * 功能：使用spark sql处理数据
    */
  def processParquet(spark: SparkSession, sql: String, outputName: String, saveMode: String)

  /**
    * 功能：将更新数据的表和原表进行merge
    */
  def mergeParquet(spark: SparkSession, baseFullname: String, update: String, keys: String, by: String, deleteTableName: String)

  /**
    * 功能：处理过程失败时，恢复之前的数据
    */
  def restoreParquet(master_url: String, spark: SparkSession, pathDir: String, restoreTable: String, backupTable: String): String

  /**
    * 功能：将sql语句中的表名替换成真正的表名
    * 参数：sql 需要parse的sql语句
    * 		 tables 读入的表
    * 返回值： 替换后的sql语句
    */
  def parseSql(spark: SparkSession, sql: String, tables: java.util.List[String]): String



}
