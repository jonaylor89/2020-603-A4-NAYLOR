
package xyz.jonaylor

import org.apache.spark.SparkContext;

object KNN {

    var k = 5
    var sc: SparkContext = null

    def main(argv: Array[String]) = {

        if (argv.length < 4) {
            println("Wrong number of arguments")
            System.exit(1)
        }

        val pathTrain = arg(1)
        val pathTest = arg(2)
        k = arg(3).toInt

        //Basic setup
        val jobName = "Naylor - KNN -> " + outDisplay + " K = " + K

        //Spark Configuration
        val conf = new SparkConf().setAppName(jobName)
        sc = new SparkContext(conf)


        val train = sc.textFile(pathTrain: String)
        val test = sc.textFile(pathTest: String)

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