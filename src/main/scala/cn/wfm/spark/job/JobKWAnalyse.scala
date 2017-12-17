package cn.wfm.spark.job

import java.io._
import java.net.URL
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.cfg.{Configuration, DefaultConfig}
import org.wltea.analyzer.core.{IKSegmenter, Lexeme}
import org.wltea.analyzer.dic.Dictionary

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.parsing.json.JSONObject
class JobKWAnalyse{


}
object JobKWAnalyse {




  //分词 功能
  def fenci(text: String): Array[String] ={
    val sr=new StringReader(text)
    //分词器
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
    //关键词 列表
    var kw: util.ArrayList[String] =new util.ArrayList[String]()
//    var br: BufferedReader = null
    var path:String = ""
    var kwName = "bg"
    var kwPath = ""
    args.length match {
      case 0 => {
       println("need at least one arg path-需要分析的源文件路径")
        return
      }
      case 1 =>{
        if(args(0) == "help"){
          println("arg1 需要分析的源文件路径")
          println("arg2 关键词文件路径")
          return
        }
        path = args(0)
        val kwUrl = this.getClass.getClassLoader.getResource("kw.dic")
        kwPath = "file://"+kwUrl.getFile
      }
      case 2 =>{
        path = args(0)
//        br = Source.fromFile(args(1)).bufferedReader()
        kwPath = args(1)
        kwName=new File(kwPath).getName.split("\\.")(0)
      }
    }
//      var lines: String = br.readLine()
//      while (lines != null) {
//        kw.add(lines)
//        lines = br.readLine()
//      }
val conf: SparkConf = new SparkConf().setAppName("skillAnalyse")
//        val conf: SparkConf = new SparkConf().setAppName("skillAnalyse").setMaster("local[*]")
    //创建spark上下文对象，也是提交任务的入口类
    val sc = new SparkContext(conf)

    val kwes = sc.textFile(kwPath).collect()
    kwes.foreach(x=>kw.add(x.toLowerCase))


    //加载 该职业的 技术 关键词
    Dictionary.initial(DefaultConfig.getInstance()).addWords(kw)



    val skillwc = sc.textFile(path).map(_.split("\\|")).filter(_.length==11).flatMap(x=>fenci(x(10)))
      .filter(x=>kw.contains(x.toLowerCase)).map((_,1)).reduceByKey(_+_).collect().toMap
//val skillwc = sc.textFile(path).map(_.split("\\|")).filter(_.length==11).count()
    val skillwcjson = JSONObject(skillwc).toString()
    println(skillwcjson)
    val name = new File(path).getName

    HbaseUtil.dataToHabse("job:jobAnalyse",name,"info","skill_"+kwName,skillwcjson)

    sc.stop()
  }
}
