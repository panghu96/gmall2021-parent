package com.aura.gmall.realtime.bean

/**
  * Author:panghu
  * Date:2021-05-20
  * Description: 三级分类存储到HBase
  */
case class Category3Name (
                         id: Long,
                         name:String,
                         category2_id: Long
                         )