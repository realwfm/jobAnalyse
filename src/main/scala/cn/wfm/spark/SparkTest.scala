package cn.wfm.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object Test {
  def main(args: Array[String]): Unit = {
    val str = Array("A B C D E F","B A C D E","C A B E","D A B E","E A B C D","F A")

    str.flatMap(x => {
      val frinds = x.split(" ")
      val res:ArrayBuffer[(String,String)] = ArrayBuffer()
      for( i <- 1 to frinds.length-1){
        for( j <- i+1 to frinds.length-1){
          res += ((frinds(i)+frinds(j),frinds(0)))
        }
      }
      res
    }).groupBy(_._1).map(x=>{(x._1,x._2.foldLeft("")(_+":"+_._2))}).foreach(x => println(x._1+"  "+x._2))

  }
}
