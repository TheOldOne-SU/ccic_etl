package net.ccic.sparkprocess

import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive._

/**
  * @Description
  * @Author franksen
  * @CREATE 2019-08-15-下午4:25
  */
class ExpandTree2UDTF extends GenericUDTF {
  var inputOIs: Array[PrimitiveObjectInspector] = null
  val tree: collection.mutable.Map[String, Option[String]] = collection.mutable.Map()

  override def initialize(args: Array[ObjectInspector]): StructObjectInspector = {
    inputOIs = args.map {
      _.asInstanceOf[PrimitiveObjectInspector]
    }
    val fieldNames = java.util.Arrays.asList("id", "ancestor", "level")
    val fieldOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector.asInstanceOf[ObjectInspector]
    val fieldOIs = java.util.Arrays.asList(fieldOI, fieldOI, fieldOI)
    ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }

  def process(record: Array[Object]) {
    val id = inputOIs(0).getPrimitiveJavaObject(record(0)).asInstanceOf[String]
    val parent = Option(inputOIs(1).getPrimitiveJavaObject(record(1)).asInstanceOf[String])
    tree += (id -> parent)
  }

  def close {
    val expandTree = collection.mutable.Map[String, List[String]]()

    def calculateAncestors(id: String): List[String] =
      tree(id) match {
        case Some(parent) => id :: getAncestors(parent);
        case None => List(id)
      }

    def getAncestors(id: String) = expandTree.getOrElseUpdate(id, calculateAncestors(id))

    tree.keys.foreach { id =>
      getAncestors(id).zipWithIndex.foreach {
        case (ancestor, level) => forward(Array(id, ancestor, level))
      }
    }
  }
}