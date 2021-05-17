package com.aura.gmall.realtime.utils

import java.util
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import redis.clients.jedis.Jedis

/**
  * Author:panghu
  * Date:2021-05-13
  * Description: 手动维护偏移量
  */
object OffsetManager {

    /**
      * 获取偏移量
      *
      * @param topicName 主题
      * @param groupId   消费者组
      * @return kafka可以识别的分区编号和对应的偏移量
      */
    def getOffset(topicName: String, groupId: String): Map[TopicPartition, Long] = {
        val jedis: Jedis = RedisUtil.getJedisClient
        //redis中key的命名方式规定：offset:主题名:消费者组
        val hashKey = "offset:" + topicName + ":" + groupId
        val offsetMap: util.Map[String, String] = jedis.hgetAll(hashKey)
        jedis.close()
        //把java中的map转为scala的map
        import scala.collection.JavaConversions._
        val topicOffset: Map[TopicPartition, Long] = offsetMap.map {
            case (partitionId, offset) => {
                println("消费偏移量起始位置：" + offset)
                (new TopicPartition(topicName, partitionId.toInt), offset.toLong)
            }
        }.toMap //转为不可变Map
        topicOffset
    }

    /**
      * 保存偏移量
      *
      * @param topicName   主题
      * @param groupId     消费者组
      * @param offsetArray 每个分区消费的最新偏移量
      */
    def submitOffset(topicName: String, groupId: String, offsetArray: Array[OffsetRange]): Unit = {
        val jedis: Jedis = RedisUtil.getJedisClient
        val partitionOffsetMap = new util.HashMap[String, String]()
        for (offset <- offsetArray) {
            val partition: Int = offset.partition
            val offsetLast: Long = offset.untilOffset
            partitionOffsetMap.put(partition.toString, offsetLast.toString)
            println("存入偏移量起始位置：" + offset.fromOffset)
            println("存入偏移量结束位置：" + offset.untilOffset)
        }

        val hashKey = "offset:" + topicName + ":" + groupId
        jedis.hmset(hashKey, partitionOffsetMap)
        jedis.close()

    }

}
