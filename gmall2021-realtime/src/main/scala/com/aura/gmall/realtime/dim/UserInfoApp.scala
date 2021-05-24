package com.aura.gmall.realtime.dim

import com.alibaba.fastjson.{JSON, JSONObject}
import com.aura.gmall.realtime.bean.{SkuInfo, UserInfo}
import com.aura.gmall.realtime.utils.{MyKafkaUtil, OffsetManager}
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}

/**
  * Author:panghu
  * Date:2021-05-21
  * Description: 
  */
object UserInfoApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //加载偏移量起始位置
        val topicName = "ODS_USER_INFO"
        val groupId = "USER_INFO_GROUP"
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
        val userInfoDStream: DStream[UserInfo] = recordDStream.map(
            record => {
                val jsonStr: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(jsonStr)
                //年龄段
                val birthday: String = jsonObj.getString("birthday")
                val birthYear: Int = birthday.split(" ")(0).split("-")(0).toInt
                val currentYear: Int = new SimpleDateFormat("yyyy").format(new Date()).toInt
                val age: Int = currentYear - birthYear
                var ageStage:String = null
                if (age < 18) {
                    ageStage = "0-17岁"
                } else if (age < 46) {
                    ageStage = "18-45岁"
                } else if (age < 70) {
                    ageStage = "46-69岁"
                } else {
                    ageStage = "70岁及以上"
                }
                //性别
                var genderName:String = null
                if (jsonObj.getString("gender") == "F") {
                    genderName = "女"
                } else {
                    genderName = "男"
                }
                UserInfo(
                    jsonObj.getLong("id"),
                    jsonObj.getString("user_level"),
                    jsonObj.getString("birthday"),
                    jsonObj.getString("gender"),
                    ageStage,
                    genderName
                )
            }
        )
        userInfoDStream.cache()

        userInfoDStream.print(1000)
        //存入HBase
        userInfoDStream.foreachRDD(
            rdd => {
                import org.apache.phoenix.spark._
                rdd.saveToPhoenix("USER_INFO0105",
                    Seq("ID", "USER_LEVEL", "BIRTHDAY", "GENDER", "USER_AGE_GROUP", "GENDER_NAME"),
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
