package com.aura.gmall.realtime.dim

import com.alibaba.fastjson.JSON
import com.aura.gmall.realtime.bean.SpuInfo
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
  * Description: 加载spu维度表到HBase
  */
object SpuInfoApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //加载偏移量起始位置
        val topicName = "ODS_SPU_INFO"
        val groupId = "SPU_INFO_GROUP"
        val offsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(topicName, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin.nonEmpty) {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        } else {
            println("offset is null")
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        }

        //加载本批次偏移量结束位置
        var offsetRanges: Array[OffsetRange] = Array.empty
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )
        //封装为对象
        val spuInfoDStream: DStream[SpuInfo] = recordDStream.map(
            record => {
                val jsonStr: String = record.value()
                val spuInfo: SpuInfo = JSON.parseObject(jsonStr, classOf[SpuInfo])
                spuInfo
            }
        )
        //存入HBase
        spuInfoDStream.foreachRDD(
            rdd => {
                import org.apache.phoenix.spark._
                rdd.saveToPhoenix("SPU_INFO0105",
                    Seq("ID", "SPU_NAME", "DESCRIPTION", "CATEGORY3_ID", "TM_ID"),
                    new Configuration,
                    Some("hadoop102,hadoop103,hadoop104:2181")
                )

                //提交偏移量
                if (offsetRanges.nonEmpty) {
                    OffsetManager.submitOffset(topicName, groupId, offsetRanges)
                }
            }
        )


        ssc.start()
        ssc.awaitTermination()
    }
}
