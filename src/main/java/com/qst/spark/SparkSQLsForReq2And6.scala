package com.qst.spark


import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQLsForReq2And6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("userinfo")
      .enableHiveSupport()
      .getOrCreate()
    spark.sql("use project02")


    //创建日志表用来存储读取的日志信息
    spark.sql("drop table if exists weblogs")
    spark.sql("create table if not exists weblogs(time string,operation string,col1 string,col2 string,col3 string,col4 string,col5 string,col6 string)row format delimited fields terminated by '|'")
    //导入数据
    spark.sql("load data inpath 'hdfs://virhost00:9000/user/test/testlogs2.txt' into table weblogs")
    spark.sql("drop table if exists orderconfirm")
    val dforder = spark.sql("select time,col1 as username,col2 as item_title,col3 as item_price,col4 as realname,col5 as province,col6 as tel from weblogs where operation = '下单成功'")
    //创建购买成功表
    dforder.write.mode(SaveMode.Overwrite).saveAsTable("orderconfirm")
    //创建用户信息表
    spark.sql("drop table if exists userinfo")
    val dfreg = spark.sql("select time,col1 as username ,col2 as age from weblogs where operation = '注册'")
    dfreg.write.mode(SaveMode.Overwrite).saveAsTable("userinfo")


    //求出每个地区对应的top3商品
    val df2 = spark.sql("select province,concat_ws(',',itemlist) items from (select province,collect_list(item_title) as itemlist from (select item_title,province,rank from (select item_title,province,row_number()over(partition by province order by count desc) as rank from (select item_title,province,count(item_title) count from orderconfirm group by item_title,province)a)b where rank <= 3) d group by province) f")
    //连接mysql数据库
    df2.write.mode("overwrite")
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.14.14:3306/project02?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC")
      .option("dbtable", "top3")
      .option("user", "root")
      .option("password", "123456")
      .option("truncate", "false").save()


    //求出每个年龄段的活跃人数
    val df3 = spark.sql("select agerange,count(agerange) as counts from (select distinct(u.username) as username, case when cast(age as int) between 0 and 18 then '0-18岁' when cast(age as int) between 19 and 25 then '19-25岁' when cast(age as int) between 26 and 35 then '26-35岁' when cast(age as int) between 36 and 45 then '36-45岁' when cast(age as int) between 46 and 60 then '46-60岁' when cast(age as int) > 60 then '60岁以上' end as agerange from weblogs w left join userinfo u on w.col1 = u.username where operation = '下单成功' and (hour(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'))-hour(w.time)+(datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), w.time))*24) <= 250) a group by agerange")
    df3.write.mode("overwrite")
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://192.168.14.14:3306/project02?useUnicode=true&characterEncoding=utf-8&serverTimezone=UTC")
      .option("dbtable", "userinfo")
      .option("user", "root")
      .option("password", "123456")
      .option("truncate", "false").save()


    spark.stop()
  }

}
