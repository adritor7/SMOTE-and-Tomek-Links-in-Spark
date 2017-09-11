package tomekLinks

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext
import utils.kNN_IS
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import utils.KeelParser

object smoteTomekLink {
  
  def runSMOTETomekLink(sc: SparkContext, 
		                    inPath: String,
                        numPartitions: Int, 
                        numReduces:Int,
                        bcTypeConv: Array[Map[String, Double]],
                        numIterations:Int,
                        outPath:String): Unit = {
      
    println("Apply Tomek-Links")
    //Structure data TomekLink
    val data = structureData.getStructureData(sc,inPath,numPartitions).cache()
    println(data.first()._1.toString())
    
    //Calculate 1NN for each instance 
    val kNN = kNN_IS.setup(data,data,1,numReduces,numIterations)
    val globalNearestNeighbors = kNN.calculatekNeighbours().cache()
    
    //Structure data LabeledPoint,Array[String]
    //LabeledPoint => data apply SMOTE
    //String => 1 Neighbour => "ID PARTITION,ID ROW"
    val dataKey = data.mapPartitions(element => element.map(instance => (instance._2.toString()+ "," + instance._3.toString(),instance._1.label)),preservesPartitioning = true).cache
    val sampleDataNearestNeighbors = globalNearestNeighbors.join(dataKey).map(x => (x._1, x._2._2, x._2._1.apply(0))).cache
    val dataArray = dataKey.mapPartitions(x => Iterator(x.toArray),preservesPartitioning = true).cache
    
    //Variables to divide the dataset
    var inc = (data.count() / numIterations).toInt
    var subdel = 0
    var topdel = inc
    if (numIterations == 1) { //If only one partition
      topdel = data.count().toInt + 1
    }
    
    //Apply Tomek-Links                                   
    val sampleDataNearestNeighborsWithKey = sampleDataNearestNeighbors.zipWithIndex().map { line => (line._2.toInt, line._1) }.sortByKey().cache      
    var dataSMOTEBroadcast: Broadcast[Array[(String,Double,String)]] = null
    var tomekLink: RDD[(String)] = null

    for (i <- 0 until numIterations) {

      if (numIterations == 1)
        dataSMOTEBroadcast = sc.broadcast(sampleDataNearestNeighbors.collect())
      else
        dataSMOTEBroadcast = sc.broadcast(sampleDataNearestNeighborsWithKey.filterByRange(subdel, topdel).map(line => line._2).collect())
      if (tomekLink == null) {
        tomekLink = dataArray.mapPartitionsWithIndex(checkTomekLink(_,_,dataSMOTEBroadcast),preservesPartitioning = true).cache()
      } else {
        tomekLink = tomekLink.union(dataArray.mapPartitionsWithIndex(checkTomekLink(_,_,dataSMOTEBroadcast),preservesPartitioning = true)).cache()
      }
      tomekLink.count

      //Update the pairs of delimiters
      subdel = topdel + 1
      topdel = topdel + inc + 1 
      //dataSMOTEBroadcast.unpersist()
    }
    
    //Remove ID-PARTITION - ID-ROW repeat
    println("Number of Tomek Link " + tomekLink.count / 2)
    val idTomekLink = tomekLink.distinct().collect()
    println("Number of instances to remove " + idTomekLink.size)
    val dataArr = data.mapPartitions(x => Iterator(x.toArray),preservesPartitioning = true).cache
    val newData = dataArr.mapPartitions(removeTomekLink(_,idTomekLink),preservesPartitioning = true)
    println("Number of instances of new dataset " + newData.count())
    //Save file
    newData.coalesce(1).saveAsTextFile(outPath)
   
  }
  
  //Find Tomek-link
  private def checkTomekLink (partitionIndex: Long,iter: Iterator[Array[(String,Double)]], sampleData: Broadcast[Array[(String,Double,String)]]): Iterator[(String)] = {
  	  var result = ArrayBuffer[(String)]()
			val dataArr = iter.next
			val nLocal = dataArr.size - 1		
			val sampleDataSize = sampleData.value.size - 1		
			for (sample <- 0 to sampleDataSize){
			    	val neighbourPartitionId = sampleData.value(sample)._3.split(",")(0).toInt
				    val neighbourRowId =  sampleData.value(sample)._3.split(",")(1).toInt
				    val label = sampleData.value(sample)._2
				    val id = sampleData.value(sample)._1
				    if (neighbourPartitionId == partitionIndex.toInt){
					      val dataNeighbour = dataArr(neighbourRowId)
					      val labelNeighbour = dataNeighbour._2
					      if(label !=  labelNeighbour){
					         result += (id)
					         result += (neighbourPartitionId + "," + neighbourRowId)
					      }
			     }
			}
		  result.iterator
  }
  
  //Remove each instance that belongs to a Tomek-link
  private def removeTomekLink(iter: Iterator[Array[(LabeledPoint,Int,Int)]], idTomekLink: Array[String]): Iterator[(String)] = {
      var result = ArrayBuffer[(String)]()
			val dataArr = iter.next
			val nLocal = dataArr.size - 1		
			val nTomekLink = idTomekLink.size-1
			for (instance <- dataArr){
			    val features = instance._1
			    val id = instance._2 + "," + instance._3
			    if(idTomekLink.contains(id) == false){
			        result += KeelParser.ParseStringLabeledPoint(features)
			    }
			}
		  result.iterator
  }
}