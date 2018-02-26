package com.knoldus

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {

  val sparkConf: SparkConf = new SparkConf().setAppName("spark-assignment-2").setMaster("local[*]")
  val sparkContext: SparkContext = new SparkContext(sparkConf)
  val file1 = sparkContext.textFile("src/main/resources/file1.txt")
  val file2 = sparkContext.textFile("src/main/resources/file2.txt")
  val file1rdd = file1.map(x => x.split('#'))
  val file2rdd = file2.map(x => x.split('#'))
  val dates = file2rdd.map(x => epochToDate(x(0).toLong))
  val newFile2rdd = file2rdd.map(x=>(x(1),x(2).toInt))
  val finalFile2rdd =  newFile2rdd zip dates
  def epochToDate(epochsec: Long): Date = {
    import java.util.Date
    val epochmillis = epochsec * 1000
    val date = new Date(epochmillis)
    date
  }
  val pairs1  = file1rdd.map(x => (x(0), x(4)))
  val pairs2 = finalFile2rdd.map{case ((x,y),z) => (x,(y,z))}
  val finalData = pairs1 join pairs2
  val outputDataByYear = finalData.map{case (w,(x,(y,z))) => (w, x, z.getYear + 1900) -> y }
    .reduceByKey(_ + _)
      .map(x => s"${x._1._2}#${x._1._3}###${x._2}")
  val outputDataByMonth = finalData.map{case (w,(x,(y,z))) => (w, x, z.getYear + 1900,z.getMonth) -> y }
    .reduceByKey(_ + _)
    .map(x => s"${x._1._2}#${x._1._3}#${x._1._4}##${x._2}")
  val outputDataByDay = finalData.map{case (w,(x,(y,z))) => (w, x, z.getYear + 1900,z.getMonth,z.getDate) -> y }
    .reduceByKey(_ + _)
    .map(x => s"${x._1._2}#${x._1._3}#${x._1._4}#${x._1._5}#${x._2}")
  val finalDataRdd = outputDataByYear union outputDataByMonth union outputDataByDay
  finalDataRdd.repartition(1).saveAsTextFile("src/main/resources/output.txt")
}
