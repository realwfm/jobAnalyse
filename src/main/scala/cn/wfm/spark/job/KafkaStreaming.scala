package cn.wfm.spark.job

import java.util

import org.apache.commons.lang.StringUtils
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.wltea.analyzer.cfg.DefaultConfig
import org.wltea.analyzer.dic.Dictionary
import redis.clients.jedis.Jedis

import scala.util.parsing.json.{JSON, JSONObject}

object KafkaStreaming {
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      case (x, y, z) => {
        (x, y.sum + z.getOrElse(0))
      }
    }
  }
//  val salaryFunc = (it: Iterator[((Int,Int),Seq[Int],Option[Int])]) => {
//    it.map{
//      case ((x,y),z,k) => {
//        ((x,y))
//      }
//    }
//  }

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    LoggerLevels.setStreamingLogLevels()
    if(args.length == 1 && args(0) == "help"){
      println("arg1 zookeeper hosts")
      println("arg2 topicgroup")
      println("arg3 topic")
      println("arg4 topic threadNum")
      println("--job :want jobName")
      println("--salaryceil :salary ceil or floor")
      println("--skillPath : need skill words to analyse ")
      return
    }
    if(args.length <= 4){
      println("need args");
      return
    }
    val Array(zkQuorum, groupId, topics, threadNum,job,skillPath) = args

    val conf = new SparkConf().setAppName("kafkajobpull").setMaster("local[2]")
    //    val conf = new SparkConf().setAppName("kafkajobpull")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
//    ssc.checkpoint("file:///C:\\Users\\75922\\Desktop\\test\\ckpoint")
    ssc.checkpoint("hdfs://192.168.216.10:9000/sparks_ck")
    val topicMap = topics.split(",").map((_, threadNum.toInt)).toMap
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicMap)
    //    val jobsource: DStream[Map[String, Int]] = data.map(_._2).map(_.split("\\|")).map(x=>{x.map((_,1)).groupBy(_._1).mapValues(_.length)})
    data.print()
    data.map(_._2).print()



    val jobsource = data.map(_._2).map(_.split("\\|"))
    //      val salary = jobsource.filter(_.length >= 5)
    //      val address = salary.filter(_.length >= 6)
    //      val companyType = address.filter(_.length >= 7)
    //      val companyPnum = companyType.filter(_.length >= 8)
    //      val experience = companyPnum.filter(_.length >= 9)
    //      val education = experience.filter(_.length >= 10)
    //      val skills = education.filter(_.length >= 11)

    //      salary.map(x=>(x(4),1)).updateStateByKey(func,new HashPartitioner(1),false)
    //    address.map(x=>(x(5),1)).updateStateByKey(func,new HashPartitioner(1),false)
    //    companyType.map(x=>(x(6),1)).updateStateByKey(func,new HashPartitioner(1),false)
    //    companyPnum.map(x=>(x(7),1)).updateStateByKey(func,new HashPartitioner(1),false)
    //    experience.map(x=>(x(8),1)).updateStateByKey(func,new HashPartitioner(1),false)
    //    education.map(x=>(x(9),1)).updateStateByKey(func,new HashPartitioner(1),false)


    val lenGt5 = jobsource.filter(_.length >=5).cache()



    //统计工资
    val salaryLevel = SalaryAnalyse.initSalaryArray
//    val salary = lenGt5.flatMap(x=>Array((SalaryAnalyse.salaryIn(x(4),salaryLevel)(salaryceil.toInt),1))).filter(_._1._1 != 0).reduceByKey(_+_)
    val salary0 = lenGt5.flatMap(x=>Array((SalaryAnalyse.salaryIn(x(4),salaryLevel)(0),1)))
      .filter(_._1._1 != 0).map(x => (x._1._1+"-"+x._1._2,x._2)).updateStateByKey(func,new HashPartitioner(1),false)


    salary0.foreachRDD(rdd => {
      val salaryMap = rdd.collect().toMap
      val salaryJson = JSONObject(salaryMap)
      val result = Map("可能实际情况下的工资"->salaryJson)
      val json = JSONObject(result)
      val jedis: Jedis = JedisConnectionPool.getConnection()
      val str: String = jedis.set(job+":可能实际情况下的工资", JSONObject(Map(job -> json.toString())).toString())
      jedis.close()
      println(json.toString())
    })

    val salary1 = lenGt5.flatMap(x=>Array((SalaryAnalyse.salaryIn(x(4),salaryLevel)(1),1)))
      .filter(_._1._1 != 0).map(x => (x._1._1+"-"+x._1._2,x._2)).updateStateByKey(func,new HashPartitioner(1),false)
    salary1.foreachRDD(rdd => {
      val salaryMap = rdd.collect().toMap
      val salaryJson = JSONObject(salaryMap)
      val result = Map("理想状态下的工资"->salaryJson)
      val json = JSONObject(result)
      val jedis: Jedis = JedisConnectionPool.getConnection()
      val str: String = jedis.set(job+":理想状态下的工资", JSONObject(Map(job -> json.toString())).toString())
      jedis.close()
      println(json.toString())
    })



    var kw: util.ArrayList[String] =new util.ArrayList[String]()
    val leneq11 = lenGt5.filter(_.length == 11).cache()
    if(skillPath != null){
      val kwes = sc.textFile(skillPath).collect()
      kwes.foreach(x=>kw.add(x.toLowerCase))

      //加载 该职业的 技术 关键词
      Dictionary.initial(DefaultConfig.getInstance()).addWords(kw)

      val skill = leneq11.flatMap(x=>JobKWAnalyse.fenci(x(10)))
        .filter(kw.contains(_)).map((_,1)).updateStateByKey(func,new HashPartitioner(1),false)

      skill.foreachRDD(rdd => {
        val skillMap = rdd.collect().toMap
        val skillJson = JSONObject(skillMap)
        val result = Map("职位要求分析"->skillJson)
        val json = JSONObject(result)

        println(json.toString())
        val jedis: Jedis = JedisConnectionPool.getConnection()
        val str: String = jedis.set(job+":职位要求分析", JSONObject(Map(job -> json.toString())).toString())
        jedis.close()

      })




    }






        val zaxaing = leneq11.flatMap(x => Array((5, x(5)), (6, x(6)), (7, x(7)), (8, x(8)), (9, x(9)))).repartition(6)
        //    val zaxaing = gt10.flatMap(x=>Array((4,x(4)),(5,x(5)),(6,x(6)),(7,x(7)),(8,x(8)),(9,x(9)))).repartition(6)
        val zares = zaxaing.mapPartitions(
          it => {
            it.toList.filter(_._2 != "").map(x => (x._1 + x._2, 1)).groupBy(_._1).mapValues(_.length).toIterator
          }).updateStateByKey(func, new HashPartitioner(6), false)

        val resMap = new util.HashMap[String, String]()

        //    zares.print()
        val jobMap = Map((5, "地址"), (6, "公司类型"), (7, "公司规模"), (8, "经验"), (9, "学历"))

    zares.foreachRDD(rdd => {
      //          val res = rdd.collect().toList.map(x=>{(jobMap(x._1.substring(0,1).toInt),(x._1.substring(1,x._1.length),x._2))}).groupBy(_._1).mapValues(x=>{JSONObject(x.map(_._2).toMap).toString()})
      //          val res = rdd.map(x=>{(jobMap(x._1.substring(0,1).toInt),(x._1.substring(1,x._1.length),x._2))}).groupBy(_._1).mapValues(x=>{JSONObject(x.map(_._2).toMap).toString()}).map(x=>JSONObject(Map(x)).toString())
      val res = rdd.map(x => {
        (jobMap(x._1.substring(0, 1).toInt), (x._1.substring(1, x._1.length), x._2))
      }).groupBy(_._1).mapValues(x => {
        JSONObject(x.map(_._2).toMap)
      })
//      println(res.collect().toBuffer)
      val valueList = res.collect().toMap
      //          val value = JSONObject(valueList).toString()
      val value = JSONObject(valueList).toString()
      val resvalue = JSONObject( Map((job -> value))).toString()

      val jedis: Jedis = JedisConnectionPool.getConnection()
      val str: String = jedis.set(job+":其他", resvalue)
      jedis.close()
      println(resvalue)
    })







/*    备份
        zares.foreachRDD(rdd => {
          //          val res = rdd.collect().toList.map(x=>{(jobMap(x._1.substring(0,1).toInt),(x._1.substring(1,x._1.length),x._2))}).groupBy(_._1).mapValues(x=>{JSONObject(x.map(_._2).toMap).toString()})
          //          val res = rdd.map(x=>{(jobMap(x._1.substring(0,1).toInt),(x._1.substring(1,x._1.length),x._2))}).groupBy(_._1).mapValues(x=>{JSONObject(x.map(_._2).toMap).toString()}).map(x=>JSONObject(Map(x)).toString())
          val res = rdd.map(x => {
            (jobMap(x._1.substring(0, 1).toInt), (x._1.substring(1, x._1.length), x._2))
          }).groupBy(_._1).mapValues(x => {
            JSONObject(x.map(_._2).toMap)
          })
          println(res.collect().toBuffer)

          val valueList = res.collect().toMap
//          val value = JSONObject(valueList).toString()
          val value = JSONObject(valueList)
          val resvalue = JSONObject( Map((job -> value))).toString()
          println(s"$job "+resvalue)
            val jedis: Jedis = JedisConnectionPool.getConnection()
            val str: String = jedis.set(job, resvalue)
            jedis.close()

        })
*/

    ssc.start()
    ssc.awaitTermination()


  }
}
class MyPartitioner(num:Int) extends Partitioner{
  override def numPartitions = num

  override def getPartition(key: Any) = {
    key match {
      case x:String =>{x(0).toInt-4}
      case _=>{0}
    }
  }
}