package net.ccic.sparkprocess.deploy

/**
  * Created by FrankSen on 2019/4/23.
  */
private[ccic] case class ArgumentMerge(
   processFileName: String,
   actionName: String,
   targetTableName: String,
   sqlText: String,
   primaryKey: String,
   deleteTableName: String
) {


}
