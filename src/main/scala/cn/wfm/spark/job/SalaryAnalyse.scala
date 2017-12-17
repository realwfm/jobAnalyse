package cn.wfm.spark.job

import java.io.File

import org.apache.hadoop.hbase.client.{Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject



object SalaryAnalyse {

  def initSalaryArray(implicit srange: (Int, Int, Int) = (0, 40000, 3000)): Array[(Int,Int)] ={
    var tmp = srange._1
    val data = ArrayBuffer[(Int, Int)]()
    while (tmp < srange._2) {
      var second = tmp + srange._3
      if(second > srange._2){
        second = srange._2
      }
      data.append((tmp, second))
      tmp = tmp + srange._3
    }
    data.toArray
  }
  def salaryIn(salary: String,data: Array[(Int,Int)])(implicit index:Int = 1):(Int, Int) = {

        val salarys: Array[String] = salary.split("-")
//        if(salary != "" && salary!= "面议"){
    try {
      if (salarys.length == 2) {
        data.map(x => {
          if (salarys(index).toInt < x._2 && salarys(index).toInt >= x._1) {
            return x
          }
        })
      }
    }catch{
      case e:Exception => {}
    }
    (0,0)
  }

  def dataToHabse(tbName:String,rowKey:String,famliy:String,qualifier:String,data:String): Unit ={
    var table: Table = HbaseUtil.getTable(tbName)
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(famliy),Bytes.toBytes(qualifier),Bytes.toBytes(data))
    table.put(put)
  }

  def main(args: Array[String]): Unit = {
    var floor:Int = 1
    var salaryRangeArg:(Int,Int,Int) = (0,40000,5000)
    var path = ""
   args.length match {
     case 0 => { print("need path arg");return}
     case 1 => {
       if(args(0) == "help"){
         println("one arg it will default 0,40000,5000,1")
         println("the second arg only allow 0")
         println("only four arg it is slaryRange （0，40000，5000）")
         println("only five arg it is slaryRange and salary ceil or floor 0，40000，5000，0")
         return
       }else{
         path = args(0)
       }
     }
     case 2 =>{
       path = args(0)
       if(args(1) == "0") floor=0 else floor =1
     }
     case 4 => path = args(0);salaryRangeArg = (args(1).toInt,args(2).toInt,args(3).toInt)
     case 5 => {
       path = args(0)
       salaryRangeArg = (args(1).toInt,args(2).toInt,args(3).toInt)
       if(args(4) == 0) floor=0 else floor =1
     }
   }

    val name = new File(path).getName
    val conf: SparkConf = new SparkConf().setAppName("salaryAnalyse")
//        val conf: SparkConf = new SparkConf().setAppName("salaryAnalyse").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)
    val lines = sc.textFile(path)
    val words = lines.map(_.split("\\|")).filter(x=>x.size==11).cache()
    val salaryArray=initSalaryArray(salaryRangeArg)
    val salaryRange: RDD[(Int, Int)] = words.map(x=>salaryIn(x(4),salaryArray)(floor))

    val typeSalary = salaryRange.map((_,1)).reduceByKey(_+_).filter(_._1._2 != 0).map(x=>(x._1._1+"-"+x._1._2,x._2))
    val res = typeSalary.collect().toMap

    val json = JSONObject(res)
    println(json.toString())
    println(name)
//    dataToHabse("job:salaryAnalyse",name+"-salary","info","salary-"+floor,json.toString())
    HbaseUtil.dataToHabse("job:jobAnalyse",name,"info","salary_"+floor,json.toString())

    val salary3 = words.filter(_.size>=10).map(x=>{(salaryIn(x(4),salaryArray)(floor),x(8),x(9))})
        .filter(x=>{x._2=="1-3年" && x._3 =="本科"}).map(x=>(x._1,1)).reduceByKey(_+_).filter(_._1._2 != 0).map(x=>(x._1._1+"-"+x._1._2,x._2)).collect().toMap
    val salary3data = JSONObject(salary3)
    println(salary3data)
    HbaseUtil.dataToHabse("job:jobAnalyse",name,"info","salary3_"+floor,salary3data.toString())

    words.unpersist()
    sc.stop()
  }
}
