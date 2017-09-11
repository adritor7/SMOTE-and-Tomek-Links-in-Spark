package smote

import org.apache.spark.SparkContext
import utils._
import scala.util.Random
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object smote {
  
  def runSMOTE(sc: SparkContext, 
		           inPath: String,
		           delimiter: String,
		           minorityClass:Int,
		           k:Int,
		           oversamplingPctg:Double,
               numPartitions: Int, 
               numReducer:Int,
               bcTypeConv: Array[Map[String, Double]],
               numIterations:Int,
               outPath:String) : Unit = {
    
    
    //Load data with structure [LabeledPoint,ID-Partition,Index-Row]
    //Obtain number of instances of the positive class
    //Obtain number of instances of the negative class
    val structure = structureData.getStructureData(sc, inPath, delimiter,minorityClass,numPartitions, bcTypeConv)
    val data = structure._1
    val num_pos = structure._2
    val num_neg = structure._3
    
    //Know how many neighbors we need
		var numberOfNeighbors = 0
    var fraction = 0.0
	 
		if(oversamplingPctg < 100){
		   fraction = ((num_neg*(oversamplingPctg.toFloat/100)).toFloat/num_pos).toFloat/2
		   numberOfNeighbors = 1
		}else{
		   numberOfNeighbors = (oversamplingPctg/100).toInt
		   fraction = (((num_neg*(oversamplingPctg.toFloat/100)).toFloat)/num_pos).toFloat/(2+numberOfNeighbors-1)
		}
    println("Number of neighbours " + numberOfNeighbors)
    println("Fraction: " + fraction)
    
    //Data to apply SMOTE
    var sampleData:RDD[(LabeledPoint,Int,Int)] = null
    if(fraction < 1){
      sampleData  = data.sample(false, fraction, 1234).cache()
    }else{
      sampleData  = data.sample(false, 1, 1234).cache()
    } 
    
    //Calculate KNN for each instance 
    val kNN = kNN_IS.setup(data,sampleData,k,numReducer,numIterations)
    val globalNearestNeighbors = kNN.calculatekNeighbours().cache()
    var sampleGlobalNearestNeighbors:RDD[(String,Array[String])] = null
    if(fraction <= 1){
      sampleGlobalNearestNeighbors = globalNearestNeighbors
    }else{
      sampleGlobalNearestNeighbors = globalNearestNeighbors.sample(true, fraction, 1234)
    }
   
    //Choose random Neighbours per Instance
    val randomNearestNeighbors = sampleGlobalNearestNeighbors.map{x => val neighbours = chooseNeighbours(numberOfNeighbors,k,x._2)
                                                                 (x._1,neighbours)
                                                                 }.cache()

    //Structure data LabeledPoint,Array[String]
    //LabeledPoint => data apply SMOTE
    //Array[String] => Neighbours => "ID PARTITION,ID ROW"
    val sampleDataKey = sampleData.mapPartitions(element => element.map(instance => (instance._2 + "," + instance._3,instance._1)),preservesPartitioning = true)
    val sampleDataNearestNeighbors = randomNearestNeighbors.join(sampleDataKey).map(x => (x._2._2, x._2._1))
    val dataArray = data.mapPartitions(x => Iterator(x.toArray),preservesPartitioning = true).cache()
    
    //Apply SMOTE
    val newData:RDD[String] = applySMOTE(sc,dataArray,sampleDataNearestNeighbors,minorityClass,oversamplingPctg,numberOfNeighbors,numIterations,delimiter) 
    println("Number of instances create " + newData.count())
    //newData.take(2).foreach(element => println(element))    
    
    //Save in file
    val instanceMayoritaryClass = sc.textFile(inPath).map(line => KeelParser.parseString(bcTypeConv, line)).filter(line => line.split(delimiter).last.compareToIgnoreCase(bcTypeConv.last.apply("negative").toString()) == 0)
    newData.union(instanceMayoritaryClass).union(sampleDataNearestNeighbors.map(instance => KeelParser.ParseStringLabeledPoint(instance._1))).coalesce(1,true).saveAsTextFile(outPath) 
  }
  
  private def applySMOTE(sc:SparkContext,data:RDD[Array[(LabeledPoint,Int,Int)]],sampleData:RDD[(LabeledPoint,Array[String])],minorityClass:Int,oversamplingPctg:Double,
                         numberOfNeighbors:Int,numIterations:Int,delimiter:String) : RDD[String] = {
    
    var inc = (sampleData.count() / numIterations).toInt
    var subdel = 0
    var topdel = inc
    if (numIterations == 1) { //If only one partition
      topdel = sampleData.count().toInt + 1
    }
                                                           
    val sampleDataNearestNeighborsWithKey = sampleData.zipWithIndex().map { line => (line._2.toInt, line._1) }.sortByKey().cache      
    var dataSMOTEBroadcast: Broadcast[Array[(LabeledPoint,Array[String])]] = null
    var newData: RDD[String] = null

    for (i <- 0 until numIterations) {

      if (numIterations == 1)
        dataSMOTEBroadcast = sc.broadcast(sampleData.collect())
      else
        dataSMOTEBroadcast = sc.broadcast(sampleDataNearestNeighborsWithKey.filterByRange(subdel, topdel).map(line => line._2).collect())
      if (newData == null) {
        newData = data.mapPartitionsWithIndex(createSyntheticData(_,_,dataSMOTEBroadcast,numberOfNeighbors,minorityClass,delimiter),preservesPartitioning = true).cache()
      } else {
        newData = newData.union(data.mapPartitionsWithIndex(createSyntheticData(_,_,dataSMOTEBroadcast,numberOfNeighbors,minorityClass,delimiter),preservesPartitioning = true)).cache()
      }
      newData.count

      //Update the pairs of delimiters
      subdel = topdel + 1
      topdel = topdel + inc + 1 
      //dataSMOTEBroadcast.unpersist()
    }
    newData
  }
  
  //Choose randomly neighbours
  private def chooseNeighbours(numberOfNeighborsSelection:Int,kNN:Int,neighbours:Array[(String)]): Array[String] = {
     var chooseNeighbours:Array[String] = null
     if (numberOfNeighborsSelection == kNN){
         return neighbours 
     }else{
       chooseNeighbours = new Array [String](numberOfNeighborsSelection)
       val rand = new Random()
       var indexNeighbour = 0
       for(position <- 0.to(numberOfNeighborsSelection-1)){
         indexNeighbour = rand.nextInt(kNN) 
         while(neighbours.contains(indexNeighbour)){
           indexNeighbour = rand.nextInt(kNN) 
         }
         chooseNeighbours(position) = neighbours(position)
       }
     }
     chooseNeighbours
	}
  
  //Create artificial instances
  private def createSyntheticData (partitionIndex: Long,iter: Iterator[Array[(LabeledPoint,Int,Int)]], sampleData: Broadcast[Array[(LabeledPoint,Array[String])]],
                                   numberOfNeighbors:Int,minorityClass:Int,delimiter:String): Iterator[String] = {
  	  var result = ArrayBuffer[String]()
			val dataArr = iter.next
			val nLocal = dataArr.size - 1			
			val sampleDataSize = sampleData.value.size - 1		
			for (sample <- 0 to sampleDataSize){
			  val sampleFeatures = sampleData.value(sample)._1.features
			  for(neighbour <- sampleData.value(sample)._2){
			    	val partitionId = neighbour.split(",")(0).toInt
				    val neighborId = neighbour.split(",")(1).toInt
				    if (partitionId == partitionIndex.toInt){
					      val currentPoint = dataArr(neighborId)	
					      val features = currentPoint._1.features	
					      val newSample = createNewSample(sampleFeatures,features)
					      result .+=(newSample.mkString(delimiter) + delimiter + minorityClass.toDouble)
				    }
			  }
			}
		  result.iterator
  }
  
  //Create a artificial instance
  private def createNewSample(x:Vector,y:Vector): Array[Double] = {
    val size = x.size
    val rand = new Random().nextDouble()
    val result:Array[Double] = new Array[Double](size)
    for(i <- 0 until size){
       val difference = (y(i)-x(i)) * rand 
       result(i) = x(i) + difference
    }
    result
  }
	
}

