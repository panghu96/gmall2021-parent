package com.aura.gmall.realtime.ods

import com.alibaba.fastjson.{JSON, JSONObject}
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
  * Description: 将maxwell监控到的数据进行分流，推到kafka ods层
  */
object BaseDBMaxwellApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val ssc = new StreamingContext(conf, Seconds(5))
        val topicName = "GMALL2021_DB_M"
        val groupId = "GMALL_MAXWELL_CONSUMER"
        //获取偏移量起始位置
        val offsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(topicName, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin == null) {
            println("offset is null")
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        } else {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        }

        //获取偏移量范围
        var offsetRangeArr = Array.empty[OffsetRange]
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRangeArr = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //处理数据并发送到kafka
        recordDStream.foreachRDD(
            rdd => {
                //转为json对象
                val jsonObjRDD: RDD[JSONObject] = rdd.map(
                    record => {
                        val jsonStr: String = record.value()
                        val jsonObj: JSONObject = JSON.parseObject(jsonStr)
                        jsonObj
                    }
                )
                //根据表名分流
                jsonObjRDD.foreachPartition(
                    ite => {
                        val jsonObjList: List[JSONObject] = ite.toList
                        for (jsonObj <- jsonObjList) {
                            val tableName: String = jsonObj.getString("table")
                            val msg: String = jsonObj.getString("data")
                            //排除maxwell bootstrap时首尾的空数据
                            if (!jsonObj.getString("type").equals("bootstrap-start")&& !jsonObj.getString("type").equals("bootstrap-complete")) {
                                val topic = "ODS_" + tableName.toUpperCase
                                println(msg)
                                //发送数据到kafka
                                MyKafkaSink.send(topic, msg)
                            }

                        }

                    }
                )

                //存储偏移量
                if (offsetRangeArr != null && offsetRangeArr.length > 0) {
                    OffsetManager.submitOffset(topicName, groupId, offsetRangeArr)
                }

            }
        )


        ssc.start()
        ssc.awaitTermination()
    }
}
