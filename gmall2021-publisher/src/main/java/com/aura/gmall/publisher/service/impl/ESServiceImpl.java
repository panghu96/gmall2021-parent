package com.aura.gmall.publisher.service.impl;

import com.aura.gmall.publisher.service.ESService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author:panghu
 * Date:2021-05-17
 * Description:
 */
@Service
public class ESServiceImpl implements ESService {

    @Autowired
    JestClient client;
    /**
     * 每日日活统计
     * @param date
     * @return
     */
    @Override
    public Long getDauTotal(String date) {
        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        String query = searchSourceBuilder.toString();
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();
        try {
            SearchResult rs = client.execute(search);
            return rs.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败！");
        }

    }

    /**
     * 每日每个时间的活跃数
     * @param date
     * @return
     */
    @Override
    public Map getDauHour(String date) {
        String indexName = "gmall_dau_info_" + date + "-query";
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder aggBuilder = AggregationBuilders.terms("group_hr").field("hr").size(24);
        String query = searchSourceBuilder.aggregation(aggBuilder).toString();
        Search search = new Search.Builder(query).addIndex(indexName).addType("_doc").build();

        try {
            SearchResult rs = client.execute(search);
            Map<String, Long> hrMap = new HashMap<>();
            TermsAggregation group_hr = rs.getAggregations().getTermsAggregation("group_hr");
            if (group_hr != null) {
                List<TermsAggregation.Entry> buckets = group_hr.getBuckets();
                for (TermsAggregation.Entry bucket : buckets) {
                    hrMap.put(bucket.getKey(), bucket.getCount());
                }
            }
            return hrMap;
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("ES查询失败！");
        }

    }
}
