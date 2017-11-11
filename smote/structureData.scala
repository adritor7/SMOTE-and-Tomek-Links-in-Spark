package smote


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import utils.KeelParser
//import utils.LabeledPoint
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ArrayBuffer

object structureData {
  
  def getStructureData(sc: SparkContext, path: String,delimiter:String , minorityClass:String,numPartitions: Int, conv: Array[Map[String, Double]]): (RDD[(LabeledPoint,Int,Int)],Long,Long) = { 
    
    val train = sc.textFile(path: String).map(line => KeelParser.parseLabeledPoint(conv, line))
    val data = train.filter(line => line.label.toString().compareToIgnoreCase(conv.last.apply(minorityClass).toString()) == 0).repartition(numPartitions).cache()
    val num_neg = train.filter(line => line.label.toString().compareToIgnoreCase(conv.last.apply(minorityClass).toString()) != 0).count()
    val num_pos = data.count()
 	  
 	  println("Number of positive examples: "+num_pos)
 	  println("Number of negative examples: "+num_neg)
 	  
 	  val formatData = data.mapPartitionsWithIndex({(partitionId, iter) =>
 	    val (iterator1, iterator2) = iter.duplicate
 	    var result = List[(LabeledPoint,Int,Int)]()
 	    var rowCount = iterator1.length - 1
 	    while(iterator2.hasNext){
 	      val it = iterator2.next
 	      result.::=((it,partitionId.toInt,rowCount))
 	      rowCount = rowCount -1
 	    }
 	    result.iterator
    },preservesPartitioning = true)
		(formatData,num_pos,num_neg)
	}
}