
package xyz.jonaylor

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext._;

object KNN {

    var k = 5
    var sc: SparkContext = null

    def main(argv: Array[String]) = {

        if (argv.length < 3) {
            println("Wrong number of arguments")
            System.exit(1)
        }

        val pathTrain = argv(0)
        val pathTest = argv(1)
        k = argv(2).toInt

        //Basic setup
        val jobName = "Naylor - KNN -> K = " + k

        //Spark Configuration
        val conf = new SparkConf().setAppName(jobName)
        sc = new SparkContext(conf)


        val train = sc.textFile(pathTrain: String)
        val test = sc.textFile(pathTest: String)

        println("HELLLLLLO WORLD")

        /*
        train.map {

        }.reduceByKey {

        }
        */

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