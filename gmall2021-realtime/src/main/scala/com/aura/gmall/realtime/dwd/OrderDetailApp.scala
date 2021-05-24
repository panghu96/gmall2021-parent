package com.aura.gmall.realtime.dwd

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.aura.gmall.realtime.bean.OrderDetail
import com.aura.gmall.realtime.utils.{MyKafkaSink, MyKafkaUtil, OffsetManager, PhoenixUtil}
import java.lang
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author:panghu
  * Date:2021-05-20
  * Description: 
  */
object OrderDetailApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //加载偏移量起始位置
        val topicName = "ODS_ORDER_DETAIL"
        val groupId = "ORDER_DETAIL"
        val offsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(topicName, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin.nonEmpty) {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        } else {
            println("offset is null")
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        }

        //获取本批次偏移量结束位置
        var offsetRanges = Array.empty[OffsetRange]
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )
        //封装为对象
        val orderDetailDStream: DStream[OrderDetail] = recordDStream.map(
            record => {
                val jsonStr: String = record.value()
                val orderDetail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])
                orderDetail
            }
        )

        //关联维表
        orderDetailDStream.foreachRDD(
            rdd => {
                //查询HBase中的维表，做成广播变量
                val skuSql = "select ID,SPU_ID,TM_ID,CATEGORY3_ID from SKU_INFO0105"
                val spuSql = "select ID,SPU_NAME from SPU_INFO0105"
                val tmSql = "select TM_ID,TM_NAME from BASE_TRADEMARK0105"
                val cate3Sql = "select ID,NAME FROM BASE_CATEGORY30105"
                val skuJsonList: List[JSONObject] = PhoenixUtil.queryList(skuSql)
                val spuJsonList: List[JSONObject] = PhoenixUtil.queryList(spuSql)
                val tmJsonList: List[JSONObject] = PhoenixUtil.queryList(tmSql)
                val cate3JsonList: List[JSONObject] = PhoenixUtil.queryList(cate3Sql)
                //转为map
                val skuMap: Map[lang.Long, JSONObject] = skuJsonList.map {
                    json => {
                        (json.getLong("ID"), json)
                    }
                }.toMap

                val spuMap: Map[lang.Long, String] = spuJsonList.map {
                    json => {
                        (json.getLong("ID"), json.getString("SPU_NAME"))
                    }
                }.toMap

                val tmMap: Map[lang.Long, String] = tmJsonList.map {
                    json => {
                        (json.getLong("TM_ID"), json.getString("TM_NAME"))
                    }
                }.toMap

                val cate3Map: Map[lang.Long, String] = cate3JsonList.map {
                    json => {
                        (json.getLong("ID"), json.getString("NAME"))
                    }
                }.toMap


                //广播
                val skuBroadcast: Broadcast[Map[lang.Long, JSONObject]] = ssc.sparkContext.broadcast(skuMap)
                val spuBroadcast: Broadcast[Map[lang.Long, String]] = ssc.sparkContext.broadcast(spuMap)
                val tmBroadcast: Broadcast[Map[lang.Long, String]] = ssc.sparkContext.broadcast(tmMap)
                val cate3Broadcast: Broadcast[Map[lang.Long, String]] = ssc.sparkContext.broadcast(cate3Map)

                //匹配
                rdd.foreachPartition(
                    orderDetailIte => {
                        val orderDetailList: List[OrderDetail] = orderDetailIte.toList
                        val skuMap: Map[lang.Long, JSONObject] = skuBroadcast.value
                        val spuMap: Map[lang.Long, String] = spuBroadcast.value
                        val tmMap: Map[lang.Long, String] = tmBroadcast.value
                        val cate3Map: Map[lang.Long, String] = cate3Broadcast.value

                        for (orderDetail <- orderDetailList) {
                            val skuJsonObj: JSONObject = skuMap.getOrElse(orderDetail.sku_id, null)
                            if (skuJsonObj != null) {
                                val spuId: lang.Long = skuJsonObj.getLong("SPU_ID")
                                val tmId: lang.Long = skuJsonObj.getLong("TM_ID")
                                val cate3Id: lang.Long = skuJsonObj.getLong("CATEGORY3_ID")

                                val spuName: String = spuMap.getOrElse(spuId, null)
                                val tmName: String = tmMap.getOrElse(tmId, null)
                                val cate3Name: String = cate3Map.getOrElse(cate3Id, null)
                                orderDetail.spu_id = spuId
                                orderDetail.tm_id = tmId
                                orderDetail.category3_id = cate3Id
                                orderDetail.spu_name = spuName
                                orderDetail.tm_name = tmName
                                orderDetail.category3_nam = cate3Name
                            }
                            //推到kafka DWD层，按照订单id分区，避免双流join发生shuffle
                            val orderDetailJson: String = JSON.toJSONString(orderDetail, new SerializeConfig(true))
                            MyKafkaSink.send("DWD_ORDER_DETAIL", orderDetail.order_id.toString, orderDetailJson)
                        }




                    }
                )

                //存储偏移量
                if (offsetRanges.nonEmpty) {
                    OffsetManager.submitOffset(topicName, groupId, offsetRanges)
                }
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
