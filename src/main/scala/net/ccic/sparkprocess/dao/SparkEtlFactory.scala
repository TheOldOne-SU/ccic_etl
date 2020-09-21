package net.ccic.sparkprocess.dao

import net.ccic.sparkprocess.deploy.{ArgumentCycleOperation, ArgumentMerge, ArgumentOverwriteAndAppend}
import net.ccic.sparkprocess.enter.impl.SparkEtlImpl._
import net.ccic.sparkprocess.utils.CataLogUtils._
import net.ccic.udf.define_ccic_udf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * Created by FrankSen on 2018/7/2.
  */
object  SparkEtlFactory {

  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  var spark: SparkSession = SparkSession
    .builder()
    .enableHiveSupport()
    .getOrCreate()
  /*
    test git
   */

  val sudf = new define_ccic_udf()

  sudf.init(spark)
  sudf.registerUDF()


  /**
    * Handle the SQL for overwrite and append actions.
    * @param overAppObj object for Argument
    */
  def executeOverwriteAndAppend(overAppObj: ArgumentOverwriteAndAppend): Unit = {

    overAppObj.actionName match {
      case "process_overwrite" =>
        printMessage("Turn on the processing table data (overwrite) mode.")
        processParquet(spark, overAppObj.sqlText, overAppObj.targetTable, "0")

      case "process_append" =>
        printMessage("Turn on the processing table data (append) mode")
        processParquet(spark, overAppObj.sqlText, overAppObj.targetTable, "1")

      case _ =>
        printMessage("No such action:" + overAppObj.actionName)
    }
    stop()
  }

  /**
    * Handle the SQL for Merge action
    * @param mergeObj object from ArgumentMerge
    */
  def executeMerge(mergeObj: ArgumentMerge): Unit ={

    mergeObj.actionName match {
      case "merge_by_name" =>
        printMessage("Enable merge data mode by name.")
        mergeParquet(spark, mergeObj.targetTableName, mergeObj.sqlText,mergeObj.primaryKey, "byName", mergeObj.deleteTableName)

      case "merge_by_sql" =>
        printMessage("Enable merge data mode by sql.")
        mergeParquet(spark, mergeObj.targetTableName, mergeObj.sqlText, mergeObj.primaryKey, "bySql", mergeObj.deleteTableName)

      case _ =>
        printMessage("No such action: " + mergeObj.actionName)
    }

    stop()
  }

  /**
    * Handle the SQL for Cycle Operation.
    * @param cycObj object for ArgumentCycleOperation
    */
  def executeCycleOperation(cycObj: ArgumentCycleOperation): Unit ={
    cycObj.actionName match {
      case "insertPartition" =>
        printMessage("Inserts data into the specified partition")
        insertPartition(cycObj.sqlText, spark)

      case _ =>
        printMessage("No such action: " + cycObj.actionName)
    }
    stop()
  }

  /**
    * Handle the SQL for other operation.
    * @param otherObj other
    */
  def executeOther(otherObj: ArgumentOverwriteAndAppend): Unit ={

    val MASTER_URL = loadProperties(otherObj.processFileName).getProperty("master.url")
    printMessage( "MASTER_URL: "+ MASTER_URL)

    otherObj.actionName match {
      case "remove" =>
        printMessage("Enable delete table mode.")
        removeParquet(MASTER_URL, spark, otherObj.sqlText, otherObj.targetTable)

        /*  case "restore" =>
        printMessage("Enable the restore table data mode.")
        val pathDir = args(2)
        val restoreTable = args(3)
        if (args.length == 4) {
          SparkEtlImpl.restoreParquet(MASTER_URL, spark, otherObj.sqlText, otherObj.targetTable, null)
        } else if (args.length == 5) {
          val backupTable = args(4)
          SparkEtlImpl.restoreParquet(MASTER_URL, spark, pathDir, restoreTable, backupTable)
        }
      case "backup" =>
        printMessage("Enable the backup date mode.")
        val pathDir = args(2)
        val tableName = args(3)
        if (args.length == 4) {
          SparkEtlImpl.backupParquet(MASTER_URL, spark, pathDir, tableName, null)
        } else if (args.length == 5) {
          if (!"".equals(args(4))) {
            SparkEtlImpl.backupParquet(MASTER_URL, spark, pathDir, tableName, args(4))
          } else {
            SparkEtlImpl.backupParquet(MASTER_URL, spark, pathDir, tableName, null)
          }

        }*/
      case _ =>
        printMessage("No such action: " + otherObj.actionName)
    }

    stop()
  }


  /**
    *  Cleans up and shuts down the SparkSession environments.
    */
  def stop(): Unit ={
    printMessage("Shutting down SparkSession Environment.")
    if(SparkEtlFactory.spark != null){
      spark.stop()
      spark = null
    }
  }

}
