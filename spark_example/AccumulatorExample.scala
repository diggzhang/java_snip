package com.onion.dataprocess

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorExample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("accTester").setMaster("local")
    val sc = new SparkContext(conf)

    val RDD = sc.parallelize(List(1, 2, 3))

    //broadcast
    val broadValue1 = sc.broadcast(2)
    val data1 = RDD.map(x => x * broadValue1.value)

    data1.foreach(x => println("broadcast value:" + x))

    var accumulator = sc.accumulator(2)

    RDD.foreach{ x =>
      print(x + " ")
      if (x < 3) {
        accumulator += 1
      }
    }
    println("accumlator is " + accumulator.value)

    sc.stop()
  }

}
