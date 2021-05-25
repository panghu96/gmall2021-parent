package com.aura.gmall.realtime.utils

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.common.TopicPartition

/**
  * Author:panghu
  * Date:2021-05-25
  * Description: 
  */
object MySQLOffsetManager {
    /**
      * 从Mysql中读取偏移量
      * @param groupId
      * @param topic
      * @return
      */
    def getOffset(groupId:String,topic:String):Map[TopicPartition,Long]={

        val offsetJsonObjList: List[JSONObject] = MySQLUtil.queryList(
            "SELECT  group_id ,topic,partition_id  , topic_offset  FROM offset_0105 where group_id='"+groupId+"' and topic='"+topic+"'")

        if(offsetJsonObjList!=null&&offsetJsonObjList.size==0){
            null
        }else {

            val kafkaOffsetList: List[(TopicPartition, Long)] = offsetJsonObjList.map { offsetJsonObj  =>
                (new TopicPartition(offsetJsonObj.getString("topic"),offsetJsonObj.getIntValue("partition_id")), offsetJsonObj.getLongValue("topic_offset"))
            }
            kafkaOffsetList.toMap
        }
    }

}
