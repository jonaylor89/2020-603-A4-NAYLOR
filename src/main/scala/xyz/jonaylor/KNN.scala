
package xyz.jonaylor

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

            val distance = euclideanDistance(testValue._1, trainValue) 
            val classification = trainValue._1[d]

            return (testValue._0, (distance, classification))
        })
    
        knnMapped.saveAsTextFile("./knnMapped")

        val knnGrouped = knnMapped.groupByKey()
        knnMapped.saveAsTextFile("./knnGrouped")

        val knnOutput = knnGrouped.mapValues(v => {
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

    def findNearest() = {

    }

    def buildClassification() = {

    }
    
    def classifyByMajority() = {

    }

        /***************************************
        MAPPING
            for t = 0 to TS_i.size() 
                CD_tj = KNN(TRj, TSj(x). k)
                results_j = (t, CD_tj)
                EMIT(result)
        ****************************************/

        /**********************************
        REDUCING
            cont = 0
            for i = 0 - k
                if result_key(cont).dist < result_reducer(i).dist 
                    result_reducer(i) = result_key(cont)
                    cont++
                end
            end
        ************************************/

    def KNN() = {

    }
}