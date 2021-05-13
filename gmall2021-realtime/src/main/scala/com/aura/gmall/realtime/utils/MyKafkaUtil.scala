package com.aura.gmall.realtime.utils


import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


/**
  * Author:panghu
  * Date:2021-05-12
  * Description:
  */
object MyKafkaUtil {
    //加载配置文件
    private val prop = PropertyUtils.load("config.properties")

    //kafka消费者配置
    val kafkaParam = collection.mutable.Map(
        "bootstrap.servers" -> prop.getProperty("kafka.broker.list"),    //kafka集群地址
        "key.deserializer" -> classOf[StringDeserializer],  //key的序列化、反序列化类型
        "value.deserializer" -> classOf[StringDeserializer],    //value的序列化、反序列化类型
        "group.id" -> "gmall2021_consumer_group",   //消费者组
        //如果偏移量没有保存或者初始化，可以使用如下配置，latest自动重置偏移量为最新
        "auto.offset.reset" -> "latest",
        //是否自动提交偏移量，false为手动维护
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )
    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题

    def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
        val dStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
        dStream
    }


    def getKafkaStream(topic: String, ssc: StreamingContext, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        kafkaParam("group.id") = groupId
        val dStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
        dStream
    }

    def getKafkaStream(topic: String, ssc: StreamingContext, offsets: Map[TopicPartition, Long], groupId: String): InputDStream[ConsumerRecord[String, String]] = {
        kafkaParam("group.id") = groupId
        val dStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam, offsets))
        dStream
    }

    //测试是否可以正常消费kafka数据
    def main(args: Array[String]): Unit = {
        //核数对应kafka的分区数即可
        val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("local[4]")
        val sc = new StreamingContext(conf, Seconds(5))
        val topic = "GMALL2021_START_LOG"
        val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, sc)
        val msgJson = kafkaDStream.map(
            record => {
                val msg: String = record.value()
                msg
            }
        )
        msgJson.print()



        sc.start()
        sc.awaitTermination()
    }
}
