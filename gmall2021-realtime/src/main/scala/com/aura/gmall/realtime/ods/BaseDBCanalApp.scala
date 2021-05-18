package com.aura.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.aura.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManager}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author:panghu
  * Date:2021-05-18
  * Description: 将canal监控到的数据进行分流，推到kafka ods层
  */
object BaseDBCanalApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val ssc = new StreamingContext(conf, Seconds(5))
        val topicName = "GMALL2021_DB_C"
        val groupId = "GMALL_CANAL_CONSUMER"

        //获取偏移量起始位置
        val offsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(topicName, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin == null) {
            println("offset is null")
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        } else {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        }

        //读取偏移量移动位置
        var offsetRangesArr = Array.empty[OffsetRange]
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRangesArr = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //处理数据并分流
        recordDStream.foreachRDD(
            rdd => {
                val JSONObjRDD: RDD[JSONObject] = rdd.map(//string数据转为json对象
                    record => {
                        val recordValue: String = record.value()
                        val jsonObj: JSONObject = JSON.parseObject(recordValue)
                        jsonObj
                    }
                )
                //根据表名进行分流
                JSONObjRDD.foreachPartition(
                    ite => {
                        val jsonList: List[JSONObject] = ite.toList
                        for (json <- jsonList) {
                            val tableName: String = json.getString("table")
                            val dataArr: JSONArray = json.getJSONArray("data")
                            for (i <- 0 until dataArr.size()) {
                                val jsonObj: JSONObject = dataArr.getJSONObject(i)
                                val msg: String = jsonObj.toJSONString
                                val topic = "ODS" + tableName.toUpperCase
                                MyKafkaSink.send(topic, msg)
                            }
                        }
                    }
                )
                //存储偏移量
                if (offsetRangesArr != null && offsetRangesArr.length > 0) {
                    OffsetManager.submitOffset(topicName, groupId, offsetRangesArr)
                }

            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
