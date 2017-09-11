package utils

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint

/** K Nearest Neighbors algorithms. 
 *  
 * @param train Traning set
 * @param k Number of neighbors
 * @param distanceType Distance.Manhattan or Distance.Euclidean
 * @param numClass Number of classes
 * @author sergiogvz
 */
class KNN(val train: ArrayBuffer[(LabeledPoint,Int,Int)], val k: Int, val distanceType: Distance.Value) {

  /** Calculates the k nearest neighbors.
   *
   * @param x Test sample
   * @return Distance and class of each nearest neighbors
   */
  def neighbors(x: (LabeledPoint,Int,Int)): (String,Array[(String,Float)]) = {
    var nearest = Array.fill(k)(-1)
    var distA = Array.fill(k)(0.0f)
    val size = train.length

    for (i <- 0 until size) { //for instance of the training set
      var dist: Float = Distance(x._1.features, train(i)._1.features, distanceType)
      if (dist > 0d) {
        var stop = false
        var j = 0
        while (j < k && !stop) { //Check if it can be inserted as NN
          if (nearest(j) == (-1) || dist <= distA(j)) {
            for (l <- ((j + 1) until k).reverse) { //for (int l = k - 1; l >= j + 1; l--)
              nearest(l) = nearest(l - 1)
              distA(l) = distA(l - 1)
            }
            nearest(j) = i
            distA(j) = dist
            stop = true
          }
          j += 1
        }
      }
    }
    val key = x._2.toString() + "," + x._3.toString()
    var out: Array[(String,Float)] = new Array[(String,Float)](k)
    for (i <- 0 until k) {
      out(i) = (train(nearest(i))._2 + "," +  train(nearest(i))._3,distA(i))
    }
    (key,out)
  }

}

/** Factory to compute the distance between two instances.
 *  
 * @author sergiogvz
 */
object Distance extends Enumeration {
  val Euclidean, Manhattan = Value

  /** Computes the (Manhattan or Euclidean) distance between instance x and instance y.
   * The type of the distance used is determined by the value of distanceType.
   *
   * @param x instance x
   * @param y instance y
   * @param distanceType type of the distance used (Distance.Euclidean or Distance.Manhattan)
   * @return Distance
   */
  def apply(x: Vector, y: Vector, distanceType: Distance.Value) = {
    distanceType match {
      case Euclidean => euclidean(x, y)
      case Manhattan => manhattan(x, y)
      case _         => euclidean(x, y)
    }
  }

  /** Computes the Euclidean distance between instance x and instance y.
   * The type of the distance used is determined by the value of distanceType.
   *
   * @param x instance x
   * @param y instance y
   * @return Euclidean distance
   */
  private def euclidean(x: Vector, y: Vector) = {
    var sum = 0.0
    val size = x.size

    for (i <- 0 until size) sum += (x(i) - y(i)) * (x(i) - y(i))

    Math.sqrt(sum).toFloat

  }

  /** Computes the Manhattan distance between instance x and instance y.
   * The type of the distance used is determined by the value of distanceType.
   *
   * @param x instance x
   * @param y instance y
   * @return Manhattan distance
   */
  private def manhattan(x: Vector, y: Vector) = {
    var sum = 0.0
    val size = x.size

    for (i <- 0 until size) sum += Math.abs(x(i) - y(i))

    sum.toFloat
  }

}