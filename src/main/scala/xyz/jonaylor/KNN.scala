
package xyz.jonaylor

import math._
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;

object KNN {

    def main(argv: Array[String]) = {

        if (argv.length < 3) {
            println("Wrong number of arguments")
            System.exit(1)
        }

        val pathTrain = argv(0)
        val K = argv(1).toInt

        //Basic setup
        val jobName = "Naylor - KNN -> K = " + k

        //Spark Configuration
        val conf = new SparkConf().setAppName(jobName)
        sc = new SparkContext(conf)


        val broadcastK = sc.broadcast(K)
        val train = sc.textFile(pathTrain: String)
        val test = train.zipWithIndex.map{case (k,v) => (v,k)}

        val cart = test.cartesian(train)
        
        val knnMapped = cart.map(case (testValue, trainValue) => { 
            val testId = testValue._0
            val testTokens = testValue._1.split(",")
            val trainTokens = trainValue.split(",")

            val distance = euclideanDistance(testTokens, trainTokens) 
            val classification = trainValue.last

            return (testID, (distance, classification))
        })
    
        knnMapped.saveAsTextFile("./knnMapped")

        val knnGrouped = knnMapped.groupByKey()
        knnMapped.saveAsTextFile("./knnGrouped")

        val knnOutput = knnGrouped.mapValues(neighbors => {
            val k = broadcastK.value 
            val nearestK = findNearest(neighbors, k)
            val majority = buildClassification(nearestK)
            val selectedClassification = classifyByMajority(majority);

            return selectedClassification;
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

    def findNearest(neighbors: Array[], k: Int) = {

    }

    def buildClassification(nearest: Array[]) = {

    }
    
    def classifyByMajority(Map[String;Int]) = {

    }
}