package com.aura.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.aura.gmall.realtime.bean.ProvinceInfo
import com.aura.gmall.realtime.utils.{MyKafkaUtil, OffsetManager}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author:panghu
  * Date:2021-05-20
  * Description: 加载地区维度表到HBase
  */
object ProvinceInfoApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //获取kafka中ODS层的province_info数据
        val topicName = "ODS_BASE_PROVINCE"
        val groupId = "GMALL_PROVINCE_INFO_GROUP"
        //加载偏移量位置
        val offsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(topicName, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin.nonEmpty) {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        } else {
            println("offset is null")
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        }

        //加载偏移量结束位置，用于更新偏移量
        var offsetRanges = Array.empty[OffsetRange]
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        val provinceInfoDStream: DStream[ProvinceInfo] = recordDStream.map(
            record => {
                val jsonStr: String = record.value()
                val provinceInfo: ProvinceInfo = JSON.parseObject(jsonStr, classOf[ProvinceInfo])
                provinceInfo
            }
        )
        provinceInfoDStream.print(1000)

        //存储到HBase
        import org.apache.phoenix.spark._
        provinceInfoDStream.foreachRDD(
            rdd => {
                rdd.saveToPhoenix("PROVINCE_INFO0105",
                    Seq("ID", "NAME", "REGION_ID", "AREA_CODE", "ISO_CODE"),
                    new Configuration, Some("hadoop102,hadoop103,hadoop104:2181"))

                //存储偏移量
                if (offsetRanges != null && offsetRanges.length > 0) {
                    OffsetManager.submitOffset(topicName, groupId, offsetRanges)
                }
            }
        )



        ssc.start()
        ssc.awaitTermination()
    }
}
