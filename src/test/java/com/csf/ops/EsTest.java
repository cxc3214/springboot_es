package com.csf.ops;

import com.csf.ops.search.dto.ReportDocDto;
import com.csf.ops.search.dto.ReportSegmentDto;
import com.csf.ops.search.service.DocumentSearchService;
import com.csf.ops.search.util.IKAnalyzerUtils;
import com.csf.ops.search.util.MapUtils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.*;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class EsTest {
    @Autowired
    ElasticsearchTemplate template;

//    @Autowired
//    ReportSearchRepository reportSearchRepository;

    @Autowired
    DocumentSearchService documentSearchService;

    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;
@Test
    public void test(){
//        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("t", "t");
//        NativeSearchQueryBuilder builder = new NativeSearchQueryBuilder().withQuery(termQueryBuilder).withIndices("research_report").withPageable(new PageRequest(0, 10));
//
//        List<String> idList = template.queryForIds(builder.build());
//        System.out.println(idList);
//    MatchQueryBuilder contentQuery3 = QueryBuilders.matchQuery("content", "消费升级");
//    BoolQueryBuilder shouldQuery = QueryBuilders.boolQuery().should(contentQuery3);
//    NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(shouldQuery).build();
//    List<String> stringList = template.queryForIds(query);
//    System.out.println(stringList);




    MatchQueryBuilder contentQuery = QueryBuilders.matchQuery("content", "消费升级");
    BoolQueryBuilder shouldQuery = QueryBuilders.boolQuery().should(contentQuery).minimumNumberShouldMatch(1);
    TermsBuilder groupbyBuilder = AggregationBuilders.terms("groupby_id").field("id");

    NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(shouldQuery)
            .withIndices("test").withTypes("type1").addAggregation(groupbyBuilder)
            .withPageable(new PageRequest(0, 10)).build();


    Aggregations aggregations = template.query(query, new ResultsExtractor<Aggregations>() {
        @Override
        public Aggregations extract(SearchResponse searchResponse) {
            return searchResponse.getAggregations();
        }
    });
    aggregations.get("groupby_id");

    Map<String, Aggregation> asMap = aggregations.getAsMap();
    System.out.println(asMap);


//    MatchQueryBuilder titleQuery = QueryBuilders.matchQuery("title", "消费升级");
//    MatchQueryBuilder contentQuery = QueryBuilders.matchQuery("content", "消费升级");
//
//    BoolQueryBuilder shouldQuery = QueryBuilders.boolQuery().should(contentQuery);
//    FieldSortBuilder dtSortBuilder = SortBuilders.fieldSort("dt").order(SortOrder.DESC);
//
//
//    TermsBuilder groupbyBuilder = AggregationBuilders.terms("groupby_id").field("id");
//    TopHitsBuilder topHis_data = AggregationBuilders.topHits("topHis_data").setSize(1);
//    groupbyBuilder.subAggregation(topHis_data);
//
//    NativeSearchQuery query = new NativeSearchQueryBuilder().withQuery(shouldQuery)
//            .withIndices("test").withTypes("type1").addAggregation(groupbyBuilder)
//            .withPageable(new PageRequest(0, 10)).build();
//
//
//    Aggregations result = template.query(query, new ResultsExtractor<Aggregations>() {
//        @Override
//        public Aggregations extract(SearchResponse searchResponse) {
//            Aggregations aggregations = searchResponse.getAggregations();
//            return aggregations;
//        }
//    });
//
//    Aggregation groupby_id = result.get("groupby_id");
//    Map<String, Object> metaData = groupby_id.getMetaData();
//    System.out.println(metaData);
//
//    System.out.println(result);



//    TermQueryBuilder idQuery = QueryBuilders.termQuery("id", "id");
//    MatchQueryBuilder titleQuery2 = QueryBuilders.matchQuery("title", "hello").operator(MatchQueryBuilder.Operator.AND);
//    MatchQueryBuilder contentQuery2 = QueryBuilders.matchQuery("content", "hello").operator(MatchQueryBuilder.Operator.AND);
//    BoolQueryBuilder shouldQuery2 = QueryBuilders.boolQuery().should(titleQuery).should(contentQuery).minimumNumberShouldMatch(1);
//    FieldSortBuilder dtSortBuilder2 = SortBuilders.fieldSort("dt").order(SortOrder.DESC);







}

@Test
    public void test2() throws Exception {
    MultiMatchQueryBuilder contentQuery = QueryBuilders.multiMatchQuery("消费升级","title","content").operator(MatchQueryBuilder.Operator.AND);
    PageRequest pageRequest = new PageRequest(0, 3);
    TermsBuilder groupbyBuilder = AggregationBuilders.terms("groupby_id").field("id");
    TopHitsBuilder topHis_data = AggregationBuilders.topHits("top_his_agg").setSize(1);
    groupbyBuilder.subAggregation(topHis_data);
    SearchQuery searchQuery = new NativeSearchQueryBuilder()
            .withQuery(contentQuery)
            .withSearchType(SearchType.DEFAULT)
            .withIndices("test").withTypes("type1")
            .addAggregation(groupbyBuilder).withPageable(pageRequest)
            .build();

    int offset = pageRequest.getOffset();
    int pageSize = pageRequest.getPageSize();

    // when
    Page<ReportDocDto> data = template.query(searchQuery, new ResultsExtractor<Page<ReportDocDto>>() {
        @Override
        public Page<ReportDocDto> extract(SearchResponse response) {
            int count = 0;
            int size = 1;
            List<ReportDocDto> result = new ArrayList<ReportDocDto>();
            StringTerms agg1 = response.getAggregations().get("groupby_id");
            List<Terms.Bucket> buckets = agg1.getBuckets();
            int totalCount = buckets.size();
            for (Terms.Bucket bucket : buckets) {
                if (size > pageSize)
                    break;
                TopHits topHits = bucket.getAggregations().get("top_his_agg");
                for (SearchHit hit : topHits.getHits()) {
                    if (count++ < offset)
                        continue;
                    if (size++ > pageSize)
                        break;
                    Map<String, Object> source = hit.getSource();
                    float score = hit.getScore();
                    source.put("score", score);
                    try {
                        ReportDocDto reportDocDto = MapUtils.convertMap(ReportDocDto.class, source);
                        result.add(reportDocDto);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            Page<ReportDocDto> reportDocDtos = new PageImpl<ReportDocDto>(result, pageRequest, totalCount);
            return reportDocDtos;
        }
    });


//        Aggregations aggregations = bucket.getAggregations();
//        Map<String, Aggregation> asMap = bucket.getAggregations().getAsMap();
//        Aggregation top_his_agg = asMap.get("top_his_agg");
//        Map<String, Object> metaData = top_his_agg.getMetaData();
//        Object title = top_his_agg.getProperty("title");
//        System.out.println(top_his_agg);



//    Map<String, Aggregation> map=aggregations.asMap();
//    for(String s:map.keySet()){
//        StringTerms a=(StringTerms) map.get(s);
//        List<Terms.Bucket> list=a.getBuckets();
//        for(Terms.Bucket b:list){
//            Aggregation top_result = b.getAggregations().get("groupby_id");
//
//            System.out.println("key is "+ b.getKeyAsString()+"---and value is "+ b.getDocCount());
//        }
//
//    }
}

@Test
    public void test3(){
//    MultiMatchQueryBuilder contentQuery = QueryBuilders.multiMatchQuery("消费升级","content","title");
//    TermsBuilder groupbyBuilder = AggregationBuilders.terms("groupby_id").field("id");
//    TopHitsBuilder topHis_data = AggregationBuilders.topHits("top_his_agg").setSize(1);
//    groupbyBuilder.subAggregation(topHis_data);
//    NativeSearchQueryBuilder searchQuery = new NativeSearchQueryBuilder()
//            .withQuery(contentQuery)
//            .withSearchType(SearchType.DEFAULT)
//            .withIndices("test").withTypes("type1").withPageable(new PageRequest(0,3))
//            .addAggregation(groupbyBuilder);
//    Page<ReportDoc> search = reportSearchRepository.search(searchQuery.build());
//    System.out.println(search);


}

@Test
    public void test1(){
//    Page<ReportDocDto> list = documentSearchService.search(new String[]{"工商银行","销售额"}, new PageRequest(0, 10));
//    System.out.println(list);

//    Page<ReportSegmentDto> list = documentSearchService.fetchDoc("report_3", new String[]{"工商银行"}, new PageRequest(0, 10));
//    System.out.println(list);


//    List<String> analyze = IKAnalyzerUtils.analyze("工商银行业绩上升，行业龙头股走势强");
//    System.out.println(analyze);
String s="子公司增值税退税";
    s="碧水源发表论文《自然》";
    s="碧水源共16个项目入围，项目涵盖水环境治理、给水污水处理、景观建设、海绵城市等方面";
    AnalyzeRequestBuilder ikRequest = new AnalyzeRequestBuilder(elasticsearchTemplate.getClient(),
            AnalyzeAction.INSTANCE,"documents",s);
    ikRequest.setTokenizer("ik_smart");
    List<AnalyzeResponse.AnalyzeToken> ikTokenList = ikRequest.execute().actionGet().getTokens();

    // 循环赋值
    List<String> searchTermList = new ArrayList<>();
    ikTokenList.forEach(ikToken -> { searchTermList.add(ikToken.getTerm()); });

    System.out.println(searchTermList);

}
}