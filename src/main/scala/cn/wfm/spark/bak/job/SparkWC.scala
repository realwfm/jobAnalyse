package cn.wfm.spark.bak.job

import java.io.StringReader

import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.core.{IKSegmenter, Lexeme}

import scala.collection.mutable.ArrayBuffer

object SparkWC {


  def fenci(text: String): Array[String] ={
    val sr=new StringReader(text)
    val  ik=new IKSegmenter(sr, false)
    var lexeme: Lexeme = ik.next()
    val res :ArrayBuffer[String] = ArrayBuffer()
    while(lexeme != null){
      //      println(lexeme.getLexemeText)
      res += lexeme.getLexemeText
      lexeme = ik.next()
    }
    res.toArray
  }
  def main(args: Array[String]): Unit = {
    //创建配置信息类,并设置应用程序名称
    //local[1] 本地启用1个线程模拟集群运行任务
    //local[*] 本地有多少空闲线程就启用多少
    val conf: SparkConf = new SparkConf().setAppName("SparkWC").setMaster("local[*]")
    //创建spark上下文对象，也是提交任务的入口类
    val sc = new SparkContext(conf)
    //读取数据
    val lines = sc.textFile(args(0))

    //处理数据

    val words = lines.flatMap(lines=>{
      val skills = lines.split("\t")
      if(skills.size>10){
        fenci(skills(10))
      }else{
        Array[String]()
      }


    })
//    println(words.collect.toBuffer)
    val tup = words.map((_,1))
    val reduced = tup.reduceByKey(_+_,1)
    val res = reduced.sortBy(_._2,false)
    res.saveAsTextFile(args(1))
    println(res.collect.toBuffer)

    sc.stop()

  }
}
