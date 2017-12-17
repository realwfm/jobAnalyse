package cn.wfm.spark.bak.job

import java.util

import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf}
import redis.clients.jedis.Jedis

import scala.util.parsing.json.JSONObject

object KafkaStreaming {
  val func = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
    it.map {
      case (x, y, z) => {
        (x, y.sum + z.getOrElse(0))
      }
    }
  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, groupId, topics, threadNum,job) = args
    val conf = new SparkConf().setAppName("kafkajobpull").setMaster("local[2]")
    //    val conf = new SparkConf().setAppName("kafkajobpull")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("file:///C:\\Users\\75922\\Desktop\\test\\ckpoint")
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


    val lenGt5 = jobsource.filter(_.length >=5).cache();
    //统计工资
    val salaryLevel = SalaryAnalyse.initSalaryArray
    val salary = lenGt5.flatMap(x=>Array(SalaryAnalyse.salaryIn(x(4),salaryLevel)));



    val leneq11 = lenGt5.filter(_.length == 11);
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
      println(res.collect().toBuffer)

      val valueList = res.collect().toMap
      val value = JSONObject(valueList).toString()
      val resvalue = JSONObject( Map((job -> value))).toString()
//      res.collect().toList.foreach(x => {
      println(s"$job "+resvalue)
        val jedis: Jedis = JedisConnectionPool.getConnection()
        val str: String = jedis.set(job, resvalue)
////        print(str)
        jedis.close()
//      })


    })

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