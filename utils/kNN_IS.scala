package utils

import org.apache.spark.SparkContext
//import breeze.linalg.Vector
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger

/**
 * Distributed kNN class.
 *
 *
 * @author Jesus Maillo
 */

class kNN_IS(train: RDD[(LabeledPoint,Int,Int)], test: RDD[(LabeledPoint,Int,Int)], k: Int, numReduces: Int, numIterations: Int) extends Serializable {

  //Count the samples of each data set and the number of classes
  private val numSamplesTrain = train.count()
  private val numSamplesTest = test.count()

  //Setting Iterative MapReduce
  private var inc = 0
  private var subdel = 0
  private var topdel = 0
  private var numIter = numIterations
  private def broadcastTest(test: Array[(LabeledPoint,Int,Int)], context: SparkContext) = context.broadcast(test)

  //Getters
  def getTrain: RDD[(LabeledPoint,Int,Int)] = train
  def getTest: RDD[(LabeledPoint,Int,Int)] = test
  def getK: Int = k
  def getNumReduces: Int = numReduces

  /**
   * Initial setting necessary. Auto-set the number of iterations and load the data sets and parameters.
   *
   * @return Instance of this class. *this*
   */
  def setup(): kNN_IS = {

    //Starting logger
    var logger = Logger.getLogger(this.getClass())

    //Setting numIterations
    numIter = numIterations

    logger.info("=> NumberIterations \"" + numIter + "\"")

    inc = (numSamplesTest / numIter).toInt
    subdel = 0
    topdel = inc
    if (numIterations == 1) { //If only one partition
      topdel = numSamplesTest.toInt + 1
    }

    this

  }

  /**
   * calculatekNeighbours. kNN
   *
   * @return RDD[(String, Array[String])]. First column String with ID PARTITION + ID ROW of each instance. Second Column Array with K neighbours. Each neighbour 
   * save a String with ID PARTITION + ID ROW
   */
  def calculatekNeighbours(): RDD[(String,Array[(String)])] = {
    val testWithKey = test.zipWithIndex().map { line => (line._2.toInt, line._1) }.sortByKey().cache
    //val testWithKey = test.sortBy(r => (r._2,r._3))
    var logger = Logger.getLogger(this.getClass())
    var testBroadcast: Broadcast[Array[(LabeledPoint,Int,Int)]] = null
    var output: RDD[(String,Array[String])] = null

    for (i <- 0 until numIter) {

      //Taking the iterative initial time.
      val timeBegIterative = System.nanoTime

      if (numIter == 1)
        testBroadcast = broadcastTest(test.collect, test.sparkContext)
      else
        testBroadcast = broadcastTest(testWithKey.filterByRange(subdel, topdel).map(line => line._2).collect, testWithKey.sparkContext)
      if (output == null) {
        output = train.mapPartitions(split => knn(split, testBroadcast, subdel)).reduceByKey(combine(_,_),numReduces).map(element => (element._2._1, element._2._2.map(neighbour => neighbour._1))).cache()
      } else {
        output = output.union(train.mapPartitions(split => knn(split, testBroadcast, subdel)).reduceByKey(combine(_,_),numReduces).map(element => (element._2._1, element._2._2.map(neighbour => neighbour._1)))).cache()
      }
      output.count

      //Update the pairs of delimiters
      subdel = topdel + 1
      topdel = topdel + inc + 1
      testBroadcast.destroy
    }

    output

  }

  /**
   * Calculate the K nearest neighbor from the test set over the train set.
   *
   * @param iter Iterator of each split of the training set.
   * @param testSet The test set in a broadcasting
   * @param subdel Int needed for take order when iterative version is running
   * @return K Nearest Neighbors for this split
   */
  def knn[T](iter: Iterator[(LabeledPoint,Int,Int)], testSet: Broadcast[Array[(LabeledPoint,Int,Int)]], subdel: Int): Iterator[(Int,(String,Array[(String,Float)]))] = {
    // Initialization
    var train = new ArrayBuffer[(LabeledPoint,Int,Int)]
    val size = testSet.value.length

    var dist: Distance.Value = Distance.Euclidean

    //Join the train set
    while (iter.hasNext)
      train.append(iter.next)

    var knnMemb = new KNN(train, k, dist)

    var auxSubDel = subdel
    var result = new Array[(Int, (String,Array[(String,Float)]))](size)

    for (i <- 0 until size) {
      result(i) = (auxSubDel, knnMemb.neighbors(testSet.value(i)))
      auxSubDel = auxSubDel + 1
    }

    result.iterator

  }

  /**
   * Join the result of the map taking the nearest neighbors.
   *
   * @param mapOut1 A element of the RDD to join
   * @param mapOut2 Another element of the RDD to join
   * @return Combine of both element with the nearest neighbors
   */
  def combine(mapOut1: (String,Array[(String,Float)]), mapOut2: (String,Array[(String,Float)])): (String,Array[(String,Float)]) = {

    var itOut1 = 0
    var itOut2 = 0
    var key = mapOut1._1
    var neighbour: Array[(String,Float)] = new Array[(String,Float)](k)

    var i = 0
    while (i < k) {
      if (mapOut1._2(itOut1)._2 <= mapOut2._2(itOut2)._2) { // Update the matrix taking the k nearest neighbors
        neighbour(i) = mapOut1._2(i)
        if (mapOut1._2(itOut1)._2 == mapOut2._2(itOut2)._2) {
          i += 1
          if (i < k) {
            neighbour(i) = mapOut2._2(i)
            itOut2 = itOut2 + 1
          }
        }
        itOut1 = itOut1 + 1
        
      } else {
        neighbour(i) = mapOut2._2(i)
        itOut2 = itOut2 + 1
      }
      i += 1
    }

    (key,neighbour)
  }
}

/**
 * Distributed kNN class.
 *
 *
 * @author Jesus Maillo
 */
object kNN_IS {
  /**
   * Initial setting necessary.
   *
   * @param train Data that iterate the RDD of the train set
   * @param test The test set in a broadcasting
   * @param k number of neighbors
   * @param distanceType MANHATTAN = 1 ; EUCLIDEAN = 2
   * @param converter Dataset's information read from the header
   * @param numPartitionMap Number of partition. Number of map tasks
   * @param numIterations Autosettins = -1. Number of split in the test set and number of iterations
   */
  def setup(train: RDD[(LabeledPoint,Int,Int)], test: RDD[(LabeledPoint,Int,Int)], k: Int, numReduces: Int, numIterations: Int) = {
    new kNN_IS(train, test, k,numReduces,numIterations).setup()
  }
}