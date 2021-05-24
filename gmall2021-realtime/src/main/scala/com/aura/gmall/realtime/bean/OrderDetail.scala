package com.aura.gmall.realtime.bean

/**
  * Author:panghu
  * Date:2021-05-20
  * Description: 
  */
case class OrderDetail(id:Long,
                       order_id:Long,
                       sku_id:Long,
                       order_price:Double,
                       sku_num:Long,
                       sku_name:String,
                       create_time:String,
                       var spu_id:Long,
                       var tm_id:Long,
                       var category3_id:Long,
                       var spu_name:String,
                       var tm_name:String,
                       var category3_nam:String)

