package cn.wfm.spark.bak.job

import java.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}


/**
  * Created by liangdmaster on 2017/6/2.
  */
object JedisConnectionPool {
  private val config: JedisPoolConfig = new JedisPoolConfig()

  //设置最大连接数
  config.setMaxTotal(10)
  //最大空闲连接数
  config.setMaxIdle(5)
  //获取连接时检查连接的有效性
  config.setTestOnBorrow(true)

  private val pool: JedisPool = new JedisPool(config, "118.89.217.144", 6379)

  //获取连接池中的一个连接
  def getConnection(): Jedis = {pool.getResource}

  def main(args: Array[String]) {
    val conn = JedisConnectionPool.getConnection()
    val keys: util.Set[String] = conn.keys("*")
    println(keys)
  }
}
