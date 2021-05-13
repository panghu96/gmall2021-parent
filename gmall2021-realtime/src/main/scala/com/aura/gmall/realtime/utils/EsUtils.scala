package com.aura.gmall.realtime.utils

import com.aura.gmall.realtime.bean.DauInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core._
import java.util
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.SortOrder

object EsUtils {
    private var factor: JestClientFactory = null


    def getConn(): JestClient = {
        if (factor == null) {
            builder()
        }
        factor.getObject
    }

    //创建连接
    def builder() = {
        factor = new JestClientFactory
        factor.setHttpClientConfig(
            //ES连接地址
            new HttpClientConfig.Builder("http://hadoop102:9200")
                    //是否多线程
                    .multiThreaded(true)
                    //最大连接数量
                    .maxTotalConnection(20)
                    //连接超时时间
                    .connTimeout(10000)
                    //读取超时时间
                    .readTimeout(10000).build()
        )

    }

    //单条插入
    def insertIndex(): Unit = {
        val jestClient: JestClient = getConn()
        //Builder设计模式
        //参数必须是可以转换为Json格式的对象
        val index = new Index.Builder(movieTest(2, "蜘蛛侠"))
                .index("movie_test01")
                .`type`("_doc")
                .id("1001")
                .build()
        jestClient.execute(index)

        jestClient.close()
    }

    //批量插入
    def bulkIndex(sourceList:List[Any], esIndex:String): Unit = {
        if (sourceList != null && sourceList.size>0){
            val jestClient: JestClient = getConn()
            val bulkBuilder = new Bulk.Builder
            for (source <- sourceList) {
                val insert: Index = new Index.Builder(source).index(esIndex).`type`("_doc").build()
                bulkBuilder.addAction(insert)
            }
            val result: BulkResult = jestClient.execute(bulkBuilder.build())
            val items: util.List[BulkResult#BulkResultItem] = result.getItems
            println("插入ES：" + items.size() + "条数据")
            jestClient.close()
        }

    }

    //查询数据
    def searchResult() = {
        val jestClient: JestClient = getConn()
        //直接查询
        val query = "{\n  \"query\": {\n    \"bool\": {\n      \"filter\": {\n        \"term\": {\n          \"actorList.name.keyword\": \"zhang han yu\"\n        }\n      },\n      \"must\": [\n        {\n          \"match\": {\n            \"name\": \"operation\"\n          }\n        }\n      ]\n    }\n  },\n  \"from\": 0,\n  \"size\": 20,\n  \"sort\": [\n    {\n      \"doubanScore\": {\n        \"order\": \"desc\"\n      }\n    }\n  ]\n}"
        //使用ES自带的API
        val searchBuilder = new SearchSourceBuilder()
        //将ES查询转换为API
        val booleanBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
        booleanBuilder.filter(new TermQueryBuilder("actorList.name.keyword","zhang han yu"))
        booleanBuilder.must(new MatchQueryBuilder("name","operation"))
        searchBuilder.from(0)
        searchBuilder.size(20)
        searchBuilder.sort("doubanScore",SortOrder.DESC)
        //API查询转换为字符串
        val query2: String = searchBuilder.toString

        val queryBuilder = new Search.Builder(query2)
                .addIndex("movie0508")
                .addType("movie")
                .build()
        val result = jestClient.execute(queryBuilder)
        //将结果封装为可以转化为json的对象，比如Map、样例类
        //Map必须是Java中的Map，因为使用的是java的API
        val hits: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String,Any]])
        import scala.collection.JavaConversions._
        val res: List[util.Map[String, Any]] = hits.map(_.source).toList
        println(res.mkString("\n"))
        jestClient.close()
    }

    def main(args: Array[String]): Unit = {
        searchResult()
    }
}

case class movieTest(id: Int, name: String)
