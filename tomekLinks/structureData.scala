package tomekLinks

import org.apache.spark.SparkContext
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import utils.KeelParser

object structureData {
  
   def getStructureData(sc:SparkContext,path:String,numPartitions: Int): RDD[(LabeledPoint,Int,Int)] = {
    
    //Obtain structure per instance: LabeledPoint,ID-Partition,ID-ROW
    val data = sc.textFile(path: String).repartition(numPartitions).cache
 	  val num_instances = data.count()
 	  println("Number of instances : " + num_instances)
 	  val formatData = data.mapPartitionsWithIndex({(partitionId, iter) =>
 	    val (iterator1, iterator2) = iter.duplicate
 	    var result = List[(LabeledPoint,Int,Int)]()
 	    var rowCount = iterator1.length - 1
 	    while(iterator2.hasNext){
 	      val it = iterator2.next
 	      val e : LabeledPoint = KeelParser.parseLabeledPointString(it)
 	      //val e : LabeledPoint = KeelParser.parseLabeledPoint(conv, it)
 	      result.::=((e,partitionId.toInt,rowCount))
 	      rowCount = rowCount -1
 	    }
 	    result.iterator
    },preservesPartitioning = true)
		formatData
	}
}