package com.aura.gmall.realtime.dws

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.aura.gmall.realtime.bean.{OrderDetail, OrderDetailWide, OrderInfo}
import com.aura.gmall.realtime.utils.{MyKafkaUtil, OffsetManager, RedisUtil}
import java.{lang, util}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import scala.collection.mutable.ListBuffer

/**
  * Author:panghu
  * Date:2021-05-21
  * Description:
  * 订单表和订单详情表双流join
  * --> 分别消费两个主题的数据，使用window滑动窗口做join
  * 注意：
  * --> 每个批次消费的订单和订单详情信息，可能会有延迟，所以使用窗口函数，跨批次join，避免丢失数据
  * --> 滑动窗口会有重复数据
  * --> dwd层的数据按照orderid进行分区，避免join时shuffle
  *
  * 求订单每件商品的实付分摊金额
  * > 如果非订单最后一件商品
  * ----> 商品实付分摊金额 = 订单实付金额 / 订单原始金额 *商品原始总额(单价*数量)
  * > 如果是订单最后一件商品
  * ----> 商品实付分摊金额 = 订单实付金额 - 【其他商品实付分摊总和】
  * > 如何判断商品是否最后一件
  * ----> 商品原始总额 = 订单原始金额 - 【其他商品原始金额总和】
  *
  * 代码：
  * > 先从redis获取 【其他商品实付分摊总和】、【其他商品原始金额总和】 ，作为判断是否最后一件商品的依据
  * > 判读是否最后一件商品，按照公式计算
  * > 将【其他商品实付分摊总和】、【其他商品原始金额总和】存入redis
  * > redis 设计：hash存储
  * ----> key：order_split_amount+order_id
  * ----> k1-v1:other_split_amount   【其他商品实付分摊总和】
  * ----> k2-v2:other_origin_amount  【其他商品原始金额总和】
  *
  */
object OrderDetailWideApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[*]")
        val ssc = new StreamingContext(conf, Seconds(5))
        //====消费订单主题====
        val orderTopic = "DWD_GMALL_ORDER_INFO"
        val orderGroupId = "ORDER_INFO_GROUP"
        val orderOffsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(orderTopic, orderGroupId)
        var orderInputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderOffsetBegin.nonEmpty) {
            orderInputDStream = MyKafkaUtil.getKafkaStream(orderTopic, ssc, orderOffsetBegin, orderGroupId)
        } else {
            orderInputDStream = MyKafkaUtil.getKafkaStream(orderTopic, ssc, orderGroupId)
        }
        //获取本次偏移量结束位置
        var orderOffsetRanges = Array.empty[OffsetRange]
        val orderRecordDStream: DStream[ConsumerRecord[String, String]] = orderInputDStream.transform(
            rdd => {
                orderOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )
        val OrderInfoDStream: DStream[OrderInfo] = orderRecordDStream.map(
            orderRecord => {
                val orderInfo: OrderInfo = JSON.parseObject(orderRecord.value(), classOf[OrderInfo])
                orderInfo
            }
        )
        val orderWindowDStream: DStream[OrderInfo] = OrderInfoDStream.window(Seconds(10), Seconds(5))

        //====消费订单详情主题====
        val orderDetailTopic = "DWD_ORDER_DETAIL"
        val orderDetailGroupId = "ORDER_DETAIL_GROUP"
        val orderDetailOffsetBegin: Map[TopicPartition, Long] = OffsetManager.getOffset(orderDetailTopic, orderDetailGroupId)
        var orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] = null
        if (orderDetailOffsetBegin.nonEmpty) {
            orderDetailInputDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailOffsetBegin, orderDetailGroupId)
        } else {
            orderDetailInputDStream = MyKafkaUtil.getKafkaStream(orderDetailTopic, ssc, orderDetailGroupId)
        }
        //获取本次偏移量结束位置
        var orderDetailOffsetRanges = Array.empty[OffsetRange]
        val orderDetailRecordDStream: DStream[ConsumerRecord[String, String]] = orderDetailInputDStream.transform(
            rdd => {
                orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                rdd
            }
        )
        val OrderDetailInfoDStream: DStream[OrderDetail] = orderDetailRecordDStream.map(
            orderDetailRecord => {
                val orderInfo: OrderDetail = JSON.parseObject(orderDetailRecord.value(), classOf[OrderDetail])
                orderInfo
            }
        )
        val orderDetailWindowDStream: DStream[OrderDetail] = OrderDetailInfoDStream.window(Seconds(10), Seconds(5))


        //====双流join====
        val orderDStreamMap: DStream[(Long, OrderInfo)] = orderWindowDStream.map(
            orderInfo => {
                (orderInfo.id, orderInfo)
            }
        )
        val orderDetailDStreamMap: DStream[(Long, OrderDetail)] = orderDetailWindowDStream.map(
            orderDetail => {
                (orderDetail.order_id, orderDetail)
            }
        )

        val joinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderDStreamMap.join(orderDetailDStreamMap)
        val orderDetailWideDStream: DStream[OrderDetailWide] = joinDStream.map {
            case (orderId, (orderInfo, orderDetail)) => {
                new OrderDetailWide(orderInfo, orderDetail)
            }
        }

        //====利用redis去重====
        val orderDetailWideFilterDStream: DStream[OrderDetailWide] = orderDetailWideDStream.mapPartitions(
            orderDetailWideIte => {
                val orderDetailWideList: List[OrderDetailWide] = orderDetailWideIte.toList
                //利用redis set数据类型去重   key  order_detail_wide+订单时间  value  订单id
                val orderDetailListBuffer = new ListBuffer[OrderDetailWide]
                val jedisClient: Jedis = RedisUtil.getJedisClient
                for (orderDetailWide <- orderDetailWideList) {
                    val redisKey: String = "order_detail_wide" + orderDetailWide.dt
                    val ifFirst: lang.Long = jedisClient.sadd(redisKey, orderDetailWide.order_id.toString)
                    //如果返回1，插入成功
                    if (ifFirst == 1) {
                        orderDetailListBuffer += orderDetailWide
                    }
                }
                jedisClient.close()

                orderDetailListBuffer.toIterator
            }
        )
        orderDetailWideFilterDStream.cache()
        orderDetailWideFilterDStream.print(1000)

        //==== 求订单每件商品的实付分摊金额 ====
        val orderSplitAmountDStream: DStream[OrderDetailWide] = orderDetailWideFilterDStream.mapPartitions(
            orderDetailWideIte => {
                val orderDetailWideList: List[OrderDetailWide] = orderDetailWideIte.toList
                //先从redis获取 【其他商品实付分摊总和】、【其他商品原始金额总和】 ，作为判断是否最后一件商品的依据
                val jedis: Jedis = RedisUtil.getJedisClient
                for (orderDetailWide <- orderDetailWideList) {
                    //key：order_split_amount+order_id
                    val redisKey: String = "order_split_amount" + orderDetailWide.order_id
                    val orderSplitAmountMap: util.Map[String, String] = jedis.hgetAll(redisKey)
                    //k1-v1:other_split_amount   【其他商品实付分摊总和】
                    //k2-v2:other_origin_amount  【其他商品原始金额总和】
                    val otherOriginAmount: String = orderSplitAmountMap.getOrDefault("other_origin_amount", "0")
                    val otherSplitAmount: String = orderSplitAmountMap.getOrDefault("other_split_amount", "0")

                    //判断是否最后一件商品
                    val currentAmount: Double = orderDetailWide.final_total_amount - otherOriginAmount.toDouble
                    var orderSplitAmount: Double = 0D
                    if (currentAmount == (orderDetailWide.sku_num * orderDetailWide.sku_price)) {
                        //商品实付分摊金额 = 订单实付金额 - 【其他商品实付分摊总和】
                        orderSplitAmount = orderDetailWide.final_total_amount - otherSplitAmount.toDouble
                        orderDetailWide.final_detail_amount = orderSplitAmount
                    } else {
                        //商品实付分摊金额 = 订单实付金额 / 订单原始金额 *商品原始总额(单价*数量)
                        orderSplitAmount =
                                Math.round((orderDetailWide.final_total_amount / orderDetailWide.original_total_amount) *
                                        (orderDetailWide.sku_price * orderDetailWide.sku_num) * 100 / 100D)
                        orderDetailWide.final_detail_amount = orderSplitAmount
                    }

                    //数据存入redis
                    //【其他商品实付分摊总和】
                    val otherSplitToRedis: Double = otherSplitAmount.toDouble + orderSplitAmount
                    jedis.hset(redisKey, "other_split_amount", otherSplitToRedis.toString)

                    //【其他商品原始金额总和】
                    val otherOriginToRedis: Double = otherOriginAmount.toDouble + orderDetailWide.sku_price * orderDetailWide.sku_num
                    jedis.hset(redisKey, "other_origin_amount", otherOriginToRedis.toString)
                }


                jedis.close()
                orderDetailWideList.toIterator
            }

        )

        orderSplitAmountDStream.map(order=>JSON.toJSONString(order, new SerializeConfig(true))).print(1000)


        //提交偏移量
        orderDetailWideFilterDStream.foreachRDD(
            rdd => {
                if (orderOffsetRanges.nonEmpty) {
                    OffsetManager.submitOffset(orderTopic, orderGroupId, orderOffsetRanges)
                }
                if (orderDetailOffsetRanges.nonEmpty) {
                    OffsetManager.submitOffset(orderDetailTopic, orderDetailGroupId, orderDetailOffsetRanges)
                }
            }
        )

        ssc.start()
        ssc.awaitTermination()
    }
}
