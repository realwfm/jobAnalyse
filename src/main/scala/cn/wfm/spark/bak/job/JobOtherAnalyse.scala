package cn.wfm.spark.bak.job

import java.io.File
import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSONObject

object JobOtherAnalyse {
  def main(args: Array[String]): Unit = {
    var salaryArg = ArrayBuffer[String]()
    var path = ""
    var skillkwArg = ArrayBuffer[String]()
    args.length match {
      case 0 => { print("need path arg");return}
      case 1 => {

          println("args need at least 1 at more 5  (path ,skillpath,(salarystart,salaryend,salarystep)")
          println("you can input  2 or 4 or 5 args")
          println("default arg :" )
          println("skillpath: bogdata keyword")
          println("salarystart,salaryend,salarystep： 0，40000，5000")
//          return
        path = args(0)
        salaryArg += path
        skillkwArg += path
      }
      case 2 =>{
        path = args(0)
        salaryArg += path
        skillkwArg += path
        skillkwArg+= args(1)
      }
      case 4 =>{
        path = args(0)
        salaryArg += path
        skillkwArg += path
        salaryArg += args(1)
        salaryArg += args(2)
        salaryArg += args(3)
      }
      case 5 => {
        path = args(0)
        salaryArg += path
        skillkwArg += path
        skillkwArg+= args(1)
        salaryArg += args(2)
        salaryArg += args(3)
        salaryArg += args(4)
      }
    }
    salaryArg.insert(1,"1")
    //工资统计
    SalaryAnalyse.main(salaryArg.toArray)
    salaryArg.remove(1)
    salaryArg.insert(1,"0")
    SalaryAnalyse.main(salaryArg.toArray)
    JobKWAnalyse.main(skillkwArg.toArray)

    val name = new File((path)).getName
//    val conf = new SparkConf().setAppName("jobAnalyse").setMaster("local[2]")
    val conf = new SparkConf().setAppName("jobAnalyse")
    val sc = new SparkContext(conf)

    val source: RDD[Array[String]] = sc.textFile(path).map(_.split("\\|")).filter(_.length>=10)

    val data = source.map(x=>(x(4),x(5),x(6),x(7),x(8),x(9))).cache()
    val datas = new util.HashMap[String,String]()
    //address wc
    val address: RDD[(String, Int)] = data.map(x=>(x._2,1)).filter(_._1!="").reduceByKey(_+_).sortBy(_._2,false)
    val addressData = JSONObject(address.collect().toMap).toString()
    datas.put("address",addressData)
    println(addressData)
//    SalaryAnalyse.dataToHabse("jobAnalyse",name+"001","info","address",addressData)

    //comparnType wc
    val ctype: RDD[(String, Int)] = data.map(x=>(x._3,1)).filter(_._1!="").reduceByKey(_+_).sortBy(_._2,false)
    val ctypeData = JSONObject(ctype.collect().toMap).toString()
    datas.put("companyType",ctypeData)
    println(ctypeData)

    //comparnPnum wc
    val pnum: RDD[(String, Int)] = data.map(x=>(x._4,1)).filter(_._1!="").reduceByKey(_+_).sortBy(_._2,false)
    val pnumData = JSONObject(pnum.collect().toMap).toString()
    datas.put("companyPnum",pnumData)
    println(pnumData)
    //experience wc
    val experience: RDD[(String, Int)] = data.map(x=>(x._5,1)).filter(_._1!="").reduceByKey(_+_).sortBy(_._2,false)
    val experienceData = JSONObject(experience.collect().toMap).toString()
    datas.put("experience",experienceData)
    println(experienceData)
    //education wc
    val education: RDD[(String, Int)] = data.map(x=>(x._6,1)).filter(_._1!="").reduceByKey(_+_).sortBy(_._2,false)
    val educationData = JSONObject(education.collect().toMap).toString()
    datas.put("education",educationData)
    println(educationData)
    HbaseUtil.dataToHabse("job:jobAnalyse",name,"info",datas)

    //取消缓存
    data.unpersist()

    sc.stop()




  }
}
