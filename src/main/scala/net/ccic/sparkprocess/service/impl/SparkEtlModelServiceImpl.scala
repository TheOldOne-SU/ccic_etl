package net.ccic.sparkprocess.service.impl

import net.ccic.sparkprocess.dao.SparkEtlFactory._
import net.ccic.sparkprocess.deploy.{ArgumentCycleOperation, ArgumentMerge, ArgumentOverwriteAndAppend}
import net.ccic.sparkprocess.service.SparkEtlModelService

/**
  * Created by FrankSen on 2018/7/2.
  */
object SparkEtlModelServiceImpl extends SparkEtlModelService{

  def sparkEtlModelService(args: Array[String]): Unit ={
    args match {
      case Array(processFileName, actionName, sqlText, targetTable) =>
         executeOverwriteAndAppend(ArgumentOverwriteAndAppend(processFileName,
           actionName,
           sqlText,
           targetTable
         ))

      case Array(processFileName, actionName, targetTable, sqlText, primaryKey, deleteTableName) =>
        executeMerge(ArgumentMerge(processFileName,
          actionName,
          targetTable,
          sqlText,
          primaryKey,
          deleteTableName
        ))

      case Array(processFileName, actionName, sqlText) =>
        executeCycleOperation(ArgumentCycleOperation(processFileName, actionName, sqlText))

      case _ => throw new Error(s"""Argument is not match,${args.mkString(",")}""")
    }
  }
}
