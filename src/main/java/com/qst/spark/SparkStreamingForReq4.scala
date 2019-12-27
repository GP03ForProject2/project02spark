package com.qst.spark

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

object SparkStreamingForReq4 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //构建conf ssc 对象
    val conf = new SparkConf().setAppName("Kafka_director").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置数据检查点进行累计统计单词
    ssc.checkpoint("./checkpointa")
    //kafka 需要Zookeeper  需要消费者组
    val topics = Set("streaming")
    //                                  broker的原信息                                  ip地址以及端口号
    val kafkaPrams = Map[String, String]("metadata.broker.list" -> "192.168.171.131:9092")
    //                                          数据的输入了类型    数据的解码类型
    val data = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaPrams, topics)
    val updateFunc = (curVal: Seq[Int], preVal: Option[Int]) => {
      //进行数据统计当前值加上之前的值
      var total = curVal.sum
      //最初的值应该是0
      var previous = preVal.getOrElse(0)
      //Some 代表最终的但会值
      Some(total + previous)
    }
    //统计结果
    data.print()
    // (null,2019-12-26 10:08:39|登陆|admin)
    val logData=data.map(_._2)
    logData.print()
    //2019-12-26 10:08:39|登陆|admin

    //注册时间
    val registTime=logData.map(_.split("\\|")).filter(_(1) == "注册").map(x=>(x(0).substring(11,13).toInt)).map((_,1)).reduceByKey(_+_)
    registTime.print()
    //10
    registTime.foreachRDD(rdd=>
      rdd.foreachPartition(eachpartition=>{
        val conn = ConnectionPool.getConnection
        eachpartition.foreach(
          x=>{
            val time=x._1
            val num=x._2
            println(x)
            val sql = "insert into registtime (time,num) values(" + " ' " + time + " ' " + " ," +" ' " + num + " ' " + ")"

            println(sql)
            val stmt = conn.createStatement()
            stmt.executeUpdate(sql)
          }
        )
        ConnectionPool.returnConnection(conn)
      })
    )

    //登录时间
    val loginTime=logData.map(_.split("\\|")).filter(_(1) == "登陆").map(x=>(x(0).substring(11,13))).map((_,1)).reduceByKey(_+_)
    loginTime.print()
    //10
    loginTime.foreachRDD(rdd=>
      rdd.foreachPartition(eachpartition=>{
        val conn = ConnectionPool.getConnection
        eachpartition.foreach(
          x=>{
            val time=x._1
            val num=x._2
            println(x)
            val sql = "insert into logintime (time,num) values(" + " ' " + time + " ' " + " ," +" ' " + num + " ' " + ")"
            //insert into logintime (time) values( " 10 " )
            println(sql)
            val stmt = conn.createStatement()
            stmt.executeUpdate(sql)
          }
        )
        ConnectionPool.returnConnection(conn)
      })
    )

    //活跃时间
    val buyTime=logData.map(_.split("\\|")).filter(_(1) == "下单成功").map(x=>(x(0).substring(11,13))).map((_,1)).reduceByKey(_+_)
    buyTime.print()
    buyTime.foreachRDD(rdd=>
      rdd.foreachPartition(eachpartition=>{
        val conn = ConnectionPool.getConnection
        eachpartition.foreach(
          x=>{
            val time=x._1
            val num=x._2
            println(x)
            val sql =  "insert into buyertime (time,num) values(" + " ' " + time + " ' " + " ," +" ' " + num + " ' " + ")"
            //insert into buytime (time) values( " 10 " )
            println(sql)
            val stmt = conn.createStatement()
            stmt.executeUpdate(sql)
          }
        )
        ConnectionPool.returnConnection(conn)
      })
    )


    //启动程序
    ssc.start()
    ssc.awaitTermination()

  }
}
