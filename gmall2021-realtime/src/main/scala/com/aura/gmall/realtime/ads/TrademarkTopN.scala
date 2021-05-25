package com.aura.gmall.realtime.ads

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.aura.gmall.realtime.bean.OrderDetailWide
import com.aura.gmall.realtime.utils.{MyKafkaUtil, MySQLOffsetManager, OffsetManager}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

/**
  * Author:panghu
  * Date:2021-05-25
  * Description: 热门品牌统计
  */
object TrademarkTopN {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //加载偏移量起始位置
        val topicName = "ADS_ORDER_WIDE"
        val groupId = "TRADEMARK_STAT_GROUP"
        val offsetBegin: Map[TopicPartition, Long] = MySQLOffsetManager.getOffset(groupId, topicName)

        //加载流
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin == null || offsetBegin.isEmpty) {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        } else {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        }

        //获取偏移量结束位置
        var offsetRanges = Array.empty[OffsetRange]
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //json转为对象
        val orderWideDStream: DStream[OrderDetailWide] = recordDStream.map(
            record => {
                val orderWideJson: String = record.value()
                val orderDetailWide: OrderDetailWide = JSON.parseObject(orderWideJson, classOf[OrderDetailWide])
                orderDetailWide
            }
        )
        orderWideDStream.cache()
        orderWideDStream.map(order => JSON.toJSONString(order,new SerializeConfig(true))).print(1000)

        //取出维度和度量字段
        val orderWideWithField: DStream[(String, Double)] = orderWideDStream.map(
            orderWide => {
                (orderWide.tm_id + ":" + orderWide.tm_name, orderWide.final_detail_amount)
            }
        )
        //聚合
        val orderWideReduce: DStream[(String, Double)] = orderWideWithField.reduceByKey(_ + _)

        //聚合结果和偏移量写入mysql
        orderWideReduce.foreachRDD(
            rdd => {
                // 结果收集到driver端
                val resultArr: Array[(String, Double)] = rdd.collect()
                if (resultArr.length > 0) {
                    //开启本地事务
                    DBs.setup()
                    DB.localTx(//localTx中的所有代码属于同一事务
                        implicit session => {
                            val statTime: String = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
                            for ((dimFields, amount) <- resultArr) {
                                val idAndName: Array[String] = dimFields.split(":")
                                val tmId: String = idAndName(0)
                                val tmName: String = idAndName(1)
                                //插入数据
                                val sql = "insert into trademark_order_detail_amount_0105 values(?,?,?,?)"
                                SQL(sql).bind(statTime, tmId, tmName, amount).update().apply()
                            }
                            //测试事务
                            //throw new RuntimeException("测试异常")
                            //提交偏移量到mysql
                            for (partitionAndOffset <- offsetRanges) {
                                val partition: Int = partitionAndOffset.partition
                                val offset: Long = partitionAndOffset.untilOffset
                                //replace有则修改，无则插入
                                val sql = "replace into offset_0105 values(?,?,?,?)"
                                SQL(sql).bind(groupId, topicName, partition, offset).update().apply()
                            }


                        }
                    )
                }


            }

        )

        ssc.start()
        ssc.awaitTermination()

    }
}
