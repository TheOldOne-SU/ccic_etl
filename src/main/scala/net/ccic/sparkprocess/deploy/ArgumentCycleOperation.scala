package net.ccic.sparkprocess.deploy

/**
  * Created by FrankSen on 2019/4/23.
  */
private[ccic] case class ArgumentCycleOperation(
    processFileName: String,
    actionName: String,
    sqlText: String
) {
}
