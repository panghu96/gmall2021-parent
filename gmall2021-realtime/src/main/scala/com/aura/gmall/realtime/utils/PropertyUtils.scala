package com.aura.gmall.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
  * Author:panghu
  * Date:2021-05-12
  * Description: 
  */
object PropertyUtils {
    def load(conf: String) = {
        val prop = new Properties()
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(conf)))
        prop
    }

    //测试是否正常加载配置文件
    def main(args: Array[String]): Unit = {
        val properties: Properties = load("config.properties")
        println(properties.getProperty("kafka.broker.list"))
    }
}
