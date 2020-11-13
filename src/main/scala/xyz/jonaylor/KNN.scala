
package xyz.jonaylor

import math._
import scala.collection.mutable.Map
import scala.collection.mutable.HashMap
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;

object KNN {

    def main(argv: Array[String]) = {

        if (argv.length < 2) {
            println("Wrong number of arguments")
            System.exit(1)
        }

        val pathTrain = argv(0)
        val K = argv(1).toInt

        //Basic setup
        val jobName = "Naylor - KNN -> K = " + K

        //Spark Configuration
        val conf = new SparkConf().setAppName(jobName)
        val sc = new SparkContext(conf)


        val broadcastK = sc.broadcast(K)
        val train = sc.textFile(pathTrain: String)
        val test = train.zipWithIndex.map{case (k,v) => (v,k)}

        val cart = test.cartesian(train)
        
        val knnMapped = cart.map{case (testValue, trainValue) => { 
                val testId = testValue._1
                val testTokens = testValue._2.split(",").map(_.toDouble)
                val trainTokens = trainValue.split(",").map(_.toDouble)

                val distance = euclideanDistance(testTokens, trainTokens) 
                val classification = trainValue.last.toInt

                (testId, (distance, classification))
            }
        }
    
        knnMapped.saveAsTextFile("./knnMapped")

        val knnGrouped = knnMapped.groupByKey()
        knnMapped.saveAsTextFile("./knnGrouped")

        val knnOutput = knnGrouped.mapValues(neighbors => {
            val k = broadcastK.value 
            val nearestK = findNearest(neighbors, k)
            val majority = buildClassification(nearestK)
            val selectedClassification = classifyByMajority(majority)

            selectedClassification
        })

        knnOutput.saveAsTextFile("./knnOutput");
    }

    def euclideanDistance(test: Array[Double], train: Array[Double]) = {
        sqrt(
            (train.dropRight(1) zip test.dropRight(1))
                    .map { case (x,y) => pow(y - x, 2) }
                    .sum
        )
    }

    def findNearest(neighbors: Iterable[Tuple2[Double,Int]], k: Int): Map[Double,Int] = {
        var nearest = new HashMap[Double,Int]() 
        neighbors.foreach{ case (distance, classification) =>

            nearest += (distance -> classification)

            if (nearest.size > k) {
                nearest.drop(k)
            }      
        }

        return nearest
    }

    def buildClassification(nearest: Map[Double,Int]): Map[Double,Int] = {
        var majority = HashMap[Double,Int]()
        
        nearest.foreach { case (distance, classification) =>
        
        }

        return majority
    }
    
    def classifyByMajority(majority: Map[Double,Int]): Int = {
        var votes: Int = 0
        var theClassification: Int = -1


        return theClassification
    }
}