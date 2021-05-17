package com.aura.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.aura.gmall.realtime.bean.DauInfo
import com.aura.gmall.realtime.utils.{EsUtils, MyKafkaUtil, OffsetManager, RedisUtil}
import java.lang
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import redis.clients.jedis.Jedis
import scala.collection.mutable.ListBuffer

/**
  * Author:panghu
  * Date:2021-05-12
  * Description: 日活统计
  */
object DauApp {
    def main(args: Array[String]): Unit = {
        //核数对应kafka的分区数即可
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
        val sc = new StreamingContext(conf, Seconds(5))

        val topic = "GMALL2021_START_LOG"
        val groupId = "GMALL_DAU_CONSUMER"
        //消费数据之前先加载偏移量
        val topicOffset: Map[TopicPartition, Long] = OffsetManager.getOffset(topic, groupId)
        var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (topicOffset != null && topicOffset.size > 0) {  //如果redis中有消费记录
            //手动指定偏移量，消费kafka数据
            kafkaDStream = MyKafkaUtil
                    .getKafkaStream(topic, sc, topicOffset, groupId)
        } else {    //首次消费
            kafkaDStream = MyKafkaUtil
                    .getKafkaStream(topic, sc)
        }

        //获取每个分区的偏移量结束位置
        var offsetArray: Array[OffsetRange] = Array.empty
        val getOffsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
            rdd => {
                offsetArray = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //将消息转为对象
        val msgDStream: DStream[JSONObject] = getOffsetDStream.map(
            record => {
                val msg: String = record.value()
                val jsonObj: JSONObject = JSON.parseObject(msg)
                val ts: lang.Long = jsonObj.getLong("ts")
                val dayAndHour: String = new SimpleDateFormat("yyyy-MM-dd HH").format(new Date(ts))
                val day_hour: Array[String] = dayAndHour.split(" ")
                jsonObj.put("day", day_hour(0))
                jsonObj.put("hour", day_hour(1))
                jsonObj
            }
        )
        //msgDStream.print()

        //利用redis对用户进行去重,利用mapPartition减少连接次数，否则一条数据就要连接一次
        val filterDStream: DStream[JSONObject] = msgDStream.mapPartitions(
            jsonIte => {
                //Iterator类型的对象只能迭代一次，多次使用需要先转成list
                val jsonObjList: List[JSONObject] = jsonIte.toList
                val jsonList = new ListBuffer[JSONObject]
                val jedis: Jedis = RedisUtil.getJedisClient
                //println("过滤前有：" + jsonObjList.size + "条数据")
                for (jsonObj <- jsonObjList) {
                    val day: String = jsonObj.getString("day")
                    val mid: String = jsonObj.getJSONObject("common").getString("mid")
                    val redis_key = "dau" + day
                    //失效日期 24h
                    jedis.expire(redis_key, 3600 * 24)
                    //利用redis的set格式去重
                    val isNew: lang.Long = jedis.sadd(redis_key, mid)
                    //如果mid不存在，就是1
                    if (isNew == 1) {
                        jsonList += jsonObj
                    }
                }
                //println("过滤后有：" + jsonList.size + "条数据")
                jedis.close()
                jsonList.toIterator
            }
        )
        //filterDStream.print(1000)

        //数据封装为case class，存储到ES
        filterDStream.foreachRDD(
            rdd => {
                rdd.foreachPartition(
                    jsonObjIte => {
                        val jsonObjList: List[JSONObject] = jsonObjIte.toList
                        //封装json
                        val dauInfoList: List[(String, DauInfo)] = jsonObjList.map(
                            jsonObj => {
                                val common: JSONObject = jsonObj.getJSONObject("common")
                                val dauInfo = DauInfo(
                                    common.getString("mid"),
                                    common.getString("uid"),
                                    common.getString("ar"),
                                    common.getString("ch"),
                                    common.getString("vc"),
                                    jsonObj.getString("day"),
                                    jsonObj.getString("hour"),
                                    "00",
                                    jsonObj.getLong("ts")
                                )
                                //mid作为ES表的id，做幂等性处理
                                (dauInfo.mid,dauInfo)
                            }
                        )

                        //前缀+今天的日期
                        val esIndex = "gmall_dau_info_" + new SimpleDateFormat("yyyy-MM-dd").format(new Date())
                        EsUtils.bulkIndex(dauInfoList, esIndex)
                    }
                )

                //偏移量保存到redis
                if (offsetArray!=null && offsetArray.length >0) {
                    OffsetManager.submitOffset(topic,groupId,offsetArray)
                }
            }
        )


        sc.start()
        sc.awaitTermination()
    }
}
