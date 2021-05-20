package com.aura.gmall.realtime.dwd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.aura.gmall.realtime.bean.{OrderInfo, ProvinceInfo, UserState}
import com.aura.gmall.realtime.utils.{MyKafkaUtil, OffsetManager, PhoenixUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Author:panghu
  * Date:2021-05-19
  * Description: 判断当日新增付费用户是否首次下单，并标记
  */
object OrderInfoApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        val ssc = new StreamingContext(conf, Seconds(5))
        //消费ODS层对应表的数据，要和保存消息的topic名字保持一致
        val topicName = "ODS_ORDER_INFO"
        val groupId = "GMALL_ORDER_INFO"
        //从redis加载偏移量起始位置
        val offsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(topicName, groupId)
        var inputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (offsetBegin != null) {
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, offsetBegin, groupId)
        } else {
            println("offset is null!")
            inputDStream = MyKafkaUtil.getKafkaStream(topicName, ssc, groupId)
        }

        //获取偏移量结束位置
        var offsetRanges = Array.empty[OffsetRange]
        val recordDStream: DStream[ConsumerRecord[String, String]] = inputDStream.transform(
            rdd => {
                offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )

        //将json封装为对象
        val orderInfoDStream: DStream[OrderInfo] = recordDStream.map(
            record => {
                val jsonStr: String = record.value()
                val orderInfo: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])
                val createTimeArr: Array[String] = orderInfo.create_time.split(" ")
                orderInfo.create_date = createTimeArr(0)
                orderInfo.create_hour = createTimeArr(1).split(":")(0)
                orderInfo
            }
        )

        //获取HBase中的用户状态，和本批次数据流中的用户做匹配
        val orderFirstDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
            orderIte => {
                val orderInfoList: List[OrderInfo] = orderIte.toList
                if (orderInfoList.nonEmpty) {
                    //user_id是varchar类型，必须有''
                    val userIds: String = orderInfoList.map("'" + _.user_id + "'").mkString(",")
                    val sql = "select USER_ID,IF_CONSUMED from USER_STATE0105 where USER_ID in (" + userIds + ")"
                    //HBase中的用户状态，筛选出uid和本批次用户相同的
                    val userStateList: List[JSONObject] = PhoenixUtil.queryList(sql)
                    val userStateMap: Map[Long, String] = userStateList.map(
                        userState => {
                            (userState.getLong("USER_ID").toLong, userState.getString("IF_CONSUMED"))
                        }
                    ).toMap
                    //HBase中的用户和本次流中的数据做匹配
                    for (orderInfo <- orderInfoList) {
                        //HBase中没有此用户，记作0
                        val ifOrderUser: String = userStateMap.getOrElse(orderInfo.user_id, "0")
                        if (ifOrderUser == 1) { //不是首次消费用户，记作0
                            orderInfo.if_first_order = "0"
                        } else {
                            orderInfo.if_first_order = "1"
                        }
                    }
                    orderInfoList.toIterator
                } else {
                    orderIte
                }
            }
        )

        //判断同一批次内是否有用户多次下单，如果有，第一次下单记作1，其他的下单记作0
        //按照user_id分组，按照时间正序排序
        val orderInfoGroupByUid: DStream[(Long, Iterable[OrderInfo])] = orderFirstDStream.map(
            orderInfo => {
                (orderInfo.user_id, orderInfo)
            }
        ).groupByKey()
        val updateDStream: DStream[OrderInfo] = orderInfoGroupByUid.flatMap {
            case (userId, orderInfoIte) => {
                val orderInfoList: List[OrderInfo] = orderInfoIte.toList
                if (orderInfoList.size > 1) {
                    val sortedOrderList: List[OrderInfo] = orderInfoList.sortWith(
                        (o1, o2) => o1.create_time < o2.create_time
                    )
                    //如果排序后第一笔订单是首次支付
                    if (orderInfoList(0).if_first_order == "1") {
                        //后面的订单都标记为0
                        for (i <- 1 until sortedOrderList.size) {
                            sortedOrderList(i).if_first_order = "0"
                        }
                    }
                    sortedOrderList
                }
                orderInfoList
            }
        }
        //将首次消费的用户存入HBase
        val userStateDStream: DStream[UserState] = updateDStream.filter(_.if_first_order == "1").map(
            orderInfo => {
                UserState(orderInfo.user_id.toString, orderInfo.if_first_order)
            }
        )
        import org.apache.phoenix.spark._
        userStateDStream.foreachRDD(
            rdd => {
                //必须导入phoenix的spark包才可以使用此算子
                rdd.saveToPhoenix("USER_STATE0105",
                    Seq("USER_ID","IF_CONSUMED"),
                    new Configuration(),
                    Some("hadoop102,hadoop103,hadoop104:2181"))

            }
        )

        //合并维度表
        //维度表的数据如果不多，而且维度表的数据使用比比较大，可以做广播变量，不用每个Executor都去HBase中查询维度表，提升性能
        orderInfoDStream.foreachRDD(
            rdd=> {
                //这里的代码在Driver端执行，而且每个批次执行一次，防止维度表更新
                val sql = " select ID,NAME,REGION_ID,AREA_CODE,ISO_CODE from PROVINCE_INFO0105"
                val jsonObjList: List[JSONObject] = PhoenixUtil.queryList(sql)
                //转为map，方便匹配查询
                val provinceMap: Map[String, ProvinceInfo] = jsonObjList.map(
                    jsonObj => {
                        val provinceInfo = ProvinceInfo(
                            jsonObj.getString("ID"),
                            jsonObj.getString("NAME"),
                            jsonObj.getString("REGION_ID"),
                            jsonObj.getString("AREA_CODE"),
                            jsonObj.getString("ISO_CODE")
                        )
                        (provinceInfo.id, provinceInfo)
                    }
                ).toMap
                //广播
                val provinceMapBroadcast: Broadcast[Map[String, ProvinceInfo]] = ssc.sparkContext.broadcast(provinceMap)

                val orderInfoWithProvince: RDD[OrderInfo] = rdd.map(
                    orderInfo => {
                        val provinceMap: Map[String, ProvinceInfo] = provinceMapBroadcast.value
                        val provinceInfo: ProvinceInfo = provinceMap.getOrElse(orderInfo.province_id.toString, null)
                        if (provinceInfo != null) {
                            orderInfo.province_name = provinceInfo.name
                            orderInfo.province_area_code = provinceInfo.area_code
                            orderInfo.province_iso_code = provinceInfo.iso_code
                        }
                        orderInfo
                    }
                )
                //orderInfoWithProvince

                //存储偏移量
                if (offsetRanges != null) {
                    OffsetManager.submitOffset(topicName,groupId,offsetRanges)
                }
            }
        )




        ssc.start()
        ssc.awaitTermination()
    }
}
