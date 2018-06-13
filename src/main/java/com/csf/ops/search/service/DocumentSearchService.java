package com.csf.ops.search.service;
import com.csf.ops.search.constants.ServiceConstants;
import com.csf.ops.search.dto.ReportDocDto;
import com.csf.ops.search.dto.ReportSegmentDto;
import com.csf.ops.search.util.ServiceUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
import org.elasticsearch.search.highlight.HighlightBuilder;
import org.elasticsearch.search.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.ResultsExtractor;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

@SuppressWarnings("Duplicates")
@Service
public class DocumentSearchService {
    @Value("${synonym.file.path}")
    private String path;
    @Autowired
    ElasticsearchTemplate elasticsearchTemplate;
    @PostConstruct
    public void init(){
        ServiceConstants.initData(path);
    }
    public Page<ReportDocDto> search(String keyword, Pageable pageRequest) {
        //对搜索词进行分词
        List<String> wordList = cutSentence(keyword);
        HashSet<String> searchKeyWords = Sets.newLinkedHashSet(wordList);
        //查找搜索内容分中是否包含公司,公司会根据company_name作为条件查询
        Map<String, Set<String>> map = fetchComAlias(searchKeyWords);
        Set<String> coms = map.keySet();

        Set<String> allWords= (Set<String>) searchKeyWords.clone();
        allWords.removeAll(coms);

        List<org.elasticsearch.index.query.QueryBuilder> segmentQueryList=new ArrayList<>();
        //公司
        List<QueryBuilder> comTermQueryList =new ArrayList<>();
        List<QueryBuilder> comQueryList_segment =new ArrayList<>();
        for (String k : map.keySet()) {
            Set<String> sets = map.get(k);
            if(CollectionUtils.isNotEmpty(sets)){
                String join = String.join(",", sets);
                TermsQueryBuilder comTermQuery = QueryBuilders.termsQuery("company_name", sets);
                comTermQueryList.add(comTermQuery);
                comQueryList_segment.add(QueryBuilders.matchQuery("segment",join).analyzer(ServiceConstants.IK_SMART));
            }
        }

        //属性，公司之外的搜索内容分会按属性查询
        Map<String, Set<String>> propMap = fetchPropAlias(allWords);
        for (String k : propMap.keySet()) {
            Set<String> sets = propMap.get(k);
            String join = String.join(",", sets);
            segmentQueryList.add(QueryBuilders.matchQuery("segment",join).analyzer(ServiceConstants.IK_SMART));
        }



        //按doc_id分组，最多去1000条
        TermsBuilder groupbyBuilder = AggregationBuilders.terms("term_agg").field("doc_id");
        TopHitsBuilder topHis_data = AggregationBuilders.topHits("top_his_agg").setSize(1);
        groupbyBuilder.subAggregation(topHis_data).size(1000);
        HighlightBuilder.Field company_name = new HighlightBuilder.Field("company_name");
        topHis_data.addHighlightedField(company_name);
        topHis_data.setExplain(true);

        //com term 公司名称条件组装
        BoolQueryBuilder boolQueryBuilder_com_term=null;
        if(CollectionUtils.isNotEmpty(comTermQueryList)){
            BoolQueryBuilder boolQueryBuilder_com_term_tmp = QueryBuilders.boolQuery();
            comTermQueryList.forEach(q->boolQueryBuilder_com_term_tmp.should(q));
            boolQueryBuilder_com_term_tmp.minimumNumberShouldMatch(1);
            boolQueryBuilder_com_term=boolQueryBuilder_com_term_tmp;
        }
        //com segment query 公司在segment中查询组装
        BoolQueryBuilder boolQueryBuilder_com_segment =null;
        if(CollectionUtils.isNotEmpty(comQueryList_segment)){
            BoolQueryBuilder boolQueryBuilder_com_segment_tmp = QueryBuilders.boolQuery();
            comQueryList_segment.forEach(q->boolQueryBuilder_com_segment_tmp.must(q));
            boolQueryBuilder_com_segment=boolQueryBuilder_com_segment_tmp;
        }

        //segment query segment查询
        BoolQueryBuilder boolQueryBuilder_segment =null;
        if(CollectionUtils.isNotEmpty(segmentQueryList)){
            BoolQueryBuilder boolQueryBuilder_segment_tmp = QueryBuilders.boolQuery();
            segmentQueryList.forEach(q->boolQueryBuilder_segment_tmp.must(q));
            boolQueryBuilder_segment=boolQueryBuilder_segment_tmp;
        }
        //封装查询builder
        BoolQueryBuilder boolQueryBuilder_segment_rs = null;
        if(boolQueryBuilder_com_segment!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_com_segment);
        }
        if(boolQueryBuilder_segment!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_segment);
        }
        if(boolQueryBuilder_com_term!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_com_term);
        }

        QueryBuilder queryBuilder=boolQueryBuilder_segment_rs==null?QueryBuilders.matchAllQuery():boolQueryBuilder_segment_rs;
        //创建查询
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(queryBuilder)
                .withSearchType(SearchType.DEFAULT)
                .withIndices(ServiceConstants.INDEX_DOCUMENTS).withTypes(ServiceConstants.TYPE_SEGMENT)
                .addAggregation(groupbyBuilder)
                .withPageable(pageRequest)
                .build();

        int offset = pageRequest.getOffset();
        int pageSize = pageRequest.getPageSize();

        // 执行查询
        Page<ReportDocDto> data = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Page<ReportDocDto>>() {
            @Override
            public Page<ReportDocDto> extract(SearchResponse response) {
                List<ReportDocDto> result = new ArrayList<ReportDocDto>();
                Aggregation groupby_id = response.getAggregations().get("term_agg");
                List<Terms.Bucket> buckets = Collections.EMPTY_LIST;
                if(groupby_id!=null && groupby_id instanceof StringTerms){
                    StringTerms agg1= (StringTerms) groupby_id;
                    buckets=agg1.getBuckets();
                }
                //获取分组结果
                int totalCount = buckets.size();
                for (Terms.Bucket bucket : buckets) {
                    TopHits topHits = bucket.getAggregations().get("top_his_agg");
                    for (SearchHit hit : topHits.getHits()) {
                        Map<String, Object> source = hit.getSource();
                        Set<String> highlightList =null;
                        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                        if(highlightFields.size()>0){
                            HighlightField com = highlightFields.get("company_name");
                            if(com!=null){
                                Text[] fragments = com.getFragments();
                                if(fragments!=null && fragments.length>0){
                                    //获取高亮关键词，做正则匹配
                                    highlightList=ServiceUtils.regexMatch(fragments[0].string(), ServiceConstants.REGEX_KEYWORD);
                                }
                            }
                        }
                        if(highlightList==null){
                            highlightList=Sets.newHashSet(source.get("company_name").toString());
                        }


                        Float score = hit.getScore();
                        source.put("score", score==null?0.0:score);
                        source.put("keyword",CollectionUtils.isNotEmpty(highlightList)?Lists.newArrayList(highlightList):Collections.EMPTY_LIST);
                        ReportDocDto reportDocDto =packingToDocumentDto(source);
                        result.add(reportDocDto);
                    }
                }
                //对查询得分归一化处理，做为匹配度
                Float max_score = CollectionUtils.isEmpty(result)?Float.NaN:result.stream().map(e -> e.getScore()).max(Comparator.comparing(e -> e)).get();
                Float min_score = CollectionUtils.isEmpty(result)?Float.NaN:result.stream().map(e -> e.getScore()).min(Comparator.comparing(e -> e)).get();
                System.out.println("max_score:"+max_score+"=======min_score:"+min_score);
                List<ReportDocDto> data = result.stream().peek(e->e.setScore(new BigDecimal((Math.atan(e.getScore())*2)/Math.PI).setScale(4, RoundingMode.DOWN).floatValue())).sorted((o1, o2) -> {
                    //无搜索内容，则按时间排序
                    int compareTo =StringUtils.isBlank(keyword)?o2.getPubtime().compareTo(o1.getPubtime()):0;
                    if(compareTo>0)
                        return 1;
                    else if(compareTo<0)
                        return -1;
                    else
                        return o2.getScore().compareTo(o1.getScore());
                }).skip(offset).limit(pageSize).collect(Collectors.toList());
                Page<ReportDocDto> reportDocDtos = new PageImpl<>(data,pageRequest,totalCount);
                return reportDocDtos;
            }
        });
        return data;
    }

    private Map<String, Set<String>> fetchPropAlias(Set<String> allWords) {
        Map<String,Set<String>> props = ServiceConstants.props;

        HashMap<String, Set<String>> result = Maps.newLinkedHashMap();
        for (String keyword : allWords) {
            if(StringUtils.isNotBlank(keyword)){
                Set<String> conts = props.entrySet().stream().filter(e -> e.getValue().contains(keyword)).map(e -> e.getKey()).collect(Collectors.toCollection(LinkedHashSet::new));
                if(CollectionUtils.isEmpty(conts)){
                    conts=Sets.newHashSet(keyword);
                }
                result.put(keyword,conts);
            }
        }

        return result;


    }

    private Map<String,Set<String>> fetchComAlias(Set<String> keywords) {
        HashMap<String, Set<String>> result = Maps.newLinkedHashMap();
        Map<String, Set<String>> coms = ServiceConstants.coms;
        for (String keyword : keywords) {
            if(StringUtils.isNotBlank(keyword)){
                Set<String> conts = coms.entrySet().stream().filter(e -> e.getValue().contains(keyword)).map(e -> e.getKey()).collect(Collectors.toCollection(LinkedHashSet::new));

                if(!CollectionUtils.isEmpty(conts)){
//                conts=Sets.newHashSet(keyword);
                    result.put(keyword,conts);
                }
            }
        }

        return result;
    }

    public Page<ReportSegmentDto> fetchDoc2(String id,String keyWord,Pageable pageable){
//        String keywords = String.join(",", keyWord);
//        keyWord=StringUtils.isBlank(keyWord)?"":keyWord.replaceAll("[ | ]+",",");
//        HashSet<String> searchKeyWords = Sets.newLinkedHashSet(Arrays.asList(keyWord.split(",")));
        HashSet<String> searchKeyWords=Sets.newLinkedHashSet(cutSentence(keyWord));
        Map<String, Set<String>> map = fetchComAlias(searchKeyWords);
        Set<String> coms = map.keySet();

        Set<String> allWords= (Set<String>) searchKeyWords.clone();
        allWords.removeAll(coms);

        List<org.elasticsearch.index.query.QueryBuilder> segmentQueryList=new ArrayList<>();
        List<org.elasticsearch.index.query.QueryBuilder> titleQueryList=new ArrayList<>();
        //公司
        List<QueryBuilder> comQueryList_segment =new ArrayList<>();
        for (String k : map.keySet()) {
            Set<String> sets = map.get(k);
            String join = String.join(",", sets);
            //TODO 修改为term
//            sets.forEach(e->comQueryList_segment.add(QueryBuilders.termQuery("segment",e)));
            if(CollectionUtils.isNotEmpty(sets)){
                TermsQueryBuilder comTermQuer = QueryBuilders.termsQuery("segment", sets);
                comQueryList_segment.add(comTermQuer);
            }
        }

        //属性
        Map<String, Set<String>> propMap = fetchPropAlias(allWords);
        for (String k : propMap.keySet()) {
            Set<String> sets = propMap.get(k);
            String join = String.join(",", sets);
            segmentQueryList.add(QueryBuilders.matchQuery("segment",join).analyzer(ServiceConstants.IK_SYNO));
        }


        //com segment query
        BoolQueryBuilder boolQueryBuilder_com_segment =null;
        if(CollectionUtils.isNotEmpty(comQueryList_segment)){
            BoolQueryBuilder boolQueryBuilder_com_segment_tmp = QueryBuilders.boolQuery();
            comQueryList_segment.forEach(q->boolQueryBuilder_com_segment_tmp.must(q));
            boolQueryBuilder_com_segment=boolQueryBuilder_com_segment_tmp;
        }

        //segment query
        BoolQueryBuilder boolQueryBuilder_segment =null;
        if(CollectionUtils.isNotEmpty(segmentQueryList)){
            BoolQueryBuilder boolQueryBuilder_segment_tmp = QueryBuilders.boolQuery();
            segmentQueryList.forEach(q->boolQueryBuilder_segment_tmp.must(q));
//            boolQueryBuilder_segment_tmp.minimumNumberShouldMatch(1);
            boolQueryBuilder_segment=boolQueryBuilder_segment_tmp;
        }
        BoolQueryBuilder boolQueryBuilder_segment_rs = null;
        if(boolQueryBuilder_com_segment!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_com_segment);
        }
        if(boolQueryBuilder_segment!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_segment);
        }

        QueryBuilder queryBuilder=boolQueryBuilder_segment_rs==null?QueryBuilders.matchAllQuery():boolQueryBuilder_segment_rs;

        HighlightBuilder.Field title =new  HighlightBuilder.Field("title");
        HighlightBuilder.Field segment = new HighlightBuilder.Field("segment");

        TermQueryBuilder idQuery = QueryBuilders.termQuery("doc_id", id);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(idQuery).must(queryBuilder);
        FieldSortBuilder pageSortBuilder = SortBuilders.fieldSort("page").order(SortOrder.ASC);
        FieldSortBuilder findIndexBuilder = SortBuilders.fieldSort("find_index").order(SortOrder.ASC);

        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(boolQueryBuilder)
                .withSearchType(SearchType.DEFAULT).withSort(pageSortBuilder).withSort(findIndexBuilder).withHighlightFields(title,segment)
                .withIndices(ServiceConstants.INDEX_DOCUMENTS).withTypes(ServiceConstants.TYPE_SEGMENT).withPageable(pageable)
                .build();

        return elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Page<ReportSegmentDto>>() {
            @Override
            public Page<ReportSegmentDto> extract(SearchResponse searchResponse) {
                List<ReportSegmentDto> result = new ArrayList<ReportSegmentDto>();
                SearchHits hits = searchResponse.getHits();
                long totalHits = hits.getTotalHits();
                hits.forEach(hit -> {
                    try {
                        Float score = hit.getScore();
                        Map<String, Object> source = hit.getSource();
                        Set<String> highlights=Sets.newLinkedHashSet();
                        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                        HighlightField segment1 = highlightFields.get("segment");
                        if(segment1!=null){
                            Text[] segment_fragment = segment1.getFragments();

                            if(segment_fragment!=null && segment_fragment.length>0){
                                for (Text text : segment_fragment) {
                                    Set<String> highlight=ServiceUtils.regexMatch(text.string(),ServiceConstants.REGEX_KEYWORD);
                                    highlights.addAll(highlight);
                                }
                            }
                        }
                        source.put("keyword",CollectionUtils.isNotEmpty(highlights)?Lists.newArrayList(highlights):Collections.EMPTY_LIST);
                        source.put("score",score==null?0.0:score);
                        result.add(packingToSegmentDto(source));
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                });
                Page<ReportSegmentDto> reportSegmentDto = new PageImpl<ReportSegmentDto>(result, pageable, totalHits);
                return reportSegmentDto;
            }
        });

    }
    public Page<ReportSegmentDto> fetchDoc(String id,String keyWord,Pageable pageable){
        //根据搜索内容分词
        HashSet<String> searchKeyWords=Sets.newLinkedHashSet(cutSentence(keyWord));
        //获取搜索内容中的公司
        Map<String, Set<String>> map = fetchComAlias(searchKeyWords);
        Set<String> coms = map.keySet();

        Set<String> allWords= (Set<String>) searchKeyWords.clone();
        allWords.removeAll(coms);

        List<org.elasticsearch.index.query.QueryBuilder> segmentQueryList=new ArrayList<>();
        //公司
        List<QueryBuilder> comQueryList_segment =new ArrayList<>();
        for (String k : map.keySet()) {
            Set<String> sets = map.get(k);
            String join = String.join(",", sets);
            if(CollectionUtils.isNotEmpty(sets)){
                TermsQueryBuilder comTermQuer = QueryBuilders.termsQuery("segment", sets);
                comQueryList_segment.add(comTermQuer);
            }
        }

        //属性 获取除公司之外的搜索内容
        Map<String, Set<String>> propMap = fetchPropAlias(allWords);
        for (String k : propMap.keySet()) {
            Set<String> sets = propMap.get(k);
            String join = String.join(",", sets);
            segmentQueryList.add(QueryBuilders.matchQuery("segment",join).analyzer(ServiceConstants.IK_SMART));
        }


        //com segment query
        BoolQueryBuilder boolQueryBuilder_com_segment =null;
        if(CollectionUtils.isNotEmpty(comQueryList_segment)){
            BoolQueryBuilder boolQueryBuilder_com_segment_tmp = QueryBuilders.boolQuery();
            comQueryList_segment.forEach(q->boolQueryBuilder_com_segment_tmp.must(q));
            boolQueryBuilder_com_segment=boolQueryBuilder_com_segment_tmp;
        }

        //segment query
        BoolQueryBuilder boolQueryBuilder_segment =null;
        if(CollectionUtils.isNotEmpty(segmentQueryList)){
            BoolQueryBuilder boolQueryBuilder_segment_tmp = QueryBuilders.boolQuery();
            segmentQueryList.forEach(q->boolQueryBuilder_segment_tmp.must(q));
            boolQueryBuilder_segment=boolQueryBuilder_segment_tmp;
        }
        BoolQueryBuilder boolQueryBuilder_segment_rs = null;
        if(boolQueryBuilder_com_segment!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_com_segment);
        }
        if(boolQueryBuilder_segment!=null){
            boolQueryBuilder_segment_rs=boolQueryBuilder_segment_rs==null?QueryBuilders.boolQuery():boolQueryBuilder_segment_rs;
            boolQueryBuilder_segment_rs.must(boolQueryBuilder_segment);
        }

        QueryBuilder queryBuilder=boolQueryBuilder_segment_rs==null?QueryBuilders.matchAllQuery():boolQueryBuilder_segment_rs;

        HighlightBuilder.Field title =new  HighlightBuilder.Field("title");
        HighlightBuilder.Field segment = new HighlightBuilder.Field("segment");

        TermQueryBuilder idQuery = QueryBuilders.termQuery("doc_id", id);

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(idQuery).must(queryBuilder);
        FieldSortBuilder pageSortBuilder = SortBuilders.fieldSort("page").order(SortOrder.ASC);
        FieldSortBuilder findIndexBuilder = SortBuilders.fieldSort("find_index").order(SortOrder.ASC);
        //创建查询
        SearchQuery searchQuery = new NativeSearchQueryBuilder()
                .withQuery(boolQueryBuilder)
                .withSearchType(SearchType.DEFAULT).withSort(pageSortBuilder).withSort(findIndexBuilder).withHighlightFields(title,segment)
                .withIndices(ServiceConstants.INDEX_DOCUMENTS).withTypes(ServiceConstants.TYPE_SEGMENT).withPageable(new PageRequest(0,1000))
                .build();
        //执行查询搜索
        return elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Page<ReportSegmentDto>>() {
            @Override
            public Page<ReportSegmentDto> extract(SearchResponse searchResponse) {
                List<ReportSegmentDto> result = new ArrayList<ReportSegmentDto>();
                SearchHits hits = searchResponse.getHits();
                long totalHits = hits.getTotalHits();
                hits.forEach(hit -> {
                    try {
                        Float score = hit.getScore();
                        Map<String, Object> source = hit.getSource();
                        Set<String> highlights=Sets.newLinkedHashSet();
                        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                        HighlightField segment1 = highlightFields.get("segment");
                        if(segment1!=null){
                            Text[] segment_fragment = segment1.getFragments();

                            if(segment_fragment!=null && segment_fragment.length>0){
                                for (Text text : segment_fragment) {
                                    Set<String> highlight=ServiceUtils.regexMatch(text.string(),ServiceConstants.REGEX_KEYWORD);
                                    highlights.addAll(highlight);
                                    source.put("keyword",CollectionUtils.isNotEmpty(highlights)?Lists.newArrayList(highlights):Collections.EMPTY_LIST);
                                    source.put("segment",text.string().replaceAll("[(<em>)|(<em/>)]",""));
                                    source.put("score",score==null?0.0:score);
                                    result.add(packingToSegmentDto(source));
                                }
                            }
                        }
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                });
                int totalCount = result.size();
                List<ReportSegmentDto> list = result.stream().collect(Collectors.toList());
                Page<ReportSegmentDto> reportSegmentDto = new PageImpl<ReportSegmentDto>(list, pageable,totalCount);
                return reportSegmentDto;
            }
        });

    }
    private ReportDocDto packingToDocumentDto(Map<String, Object> source) {
        ReportDocDto reportDocDto = new ReportDocDto();
        Object file_url = source.get("file_url");
        reportDocDto.setUrl(file_url==null?"":file_url.toString());
        reportDocDto.setKeyword((List<String>)(source.get("keyword")));
        Object score = source.get("score");
        reportDocDto.setScore(Float.valueOf(score==null?"0":score.toString()));
        Object file_type = source.get("content_type");
        reportDocDto.setType(file_type==null?"":file_type.toString());
        Object pub_date = source.get("pub_date");
        reportDocDto.setPubtime(pub_date==null?"":pub_date.toString());
        Object company_code = source.get("company_code");
        reportDocDto.setCode(company_code==null?"":company_code.toString());
        Object id = source.get("doc_id");
        reportDocDto.setId(id==null?"":id.toString());
        Object company_name = source.get("company_name");
        reportDocDto.setName(company_name==null?"":company_name.toString());
        Object title = source.get("title");
        reportDocDto.setTitle(title==null?"":title.toString());
//        Object segment = source.get("segment");
//        reportDocDto.setSegment(segment==null?"":segment.toString());

        return reportDocDto;
    }
    private ReportSegmentDto packingToSegmentDto(Map<String, Object> source) {
        ReportSegmentDto reportSegmentDto = new ReportSegmentDto();
        Object id = source.get("doc_id");
        reportSegmentDto.setId(id.toString());
        Object company_code = source.get("company_code");
        reportSegmentDto.setCode(company_code==null?"":company_code.toString());
        Object file_type = source.get("content_type");
        reportSegmentDto.setType(file_type==null?"":file_type.toString());
        Object title = source.get("title");
        reportSegmentDto.setTitle(title==null?"":title.toString());
        Object file_index = source.get("find_index");
        reportSegmentDto.setIdx(file_index==null?0:Integer.valueOf(file_index.toString()));
        reportSegmentDto.setKeyword((List<String>)(source.get("keyword")));
        Object page = source.get("page");
        reportSegmentDto.setPage(Integer.valueOf(page==null?"0":page.toString()));
        Object company_name = source.get("company_name");
        reportSegmentDto.setName(company_name==null?"":company_name.toString());
        Object pub_date = source.get("pub_date");
        reportSegmentDto.setPubtime(pub_date==null?"":pub_date.toString());
        Object score = source.get("score");
        reportSegmentDto.setScore(Float.valueOf(score==null?"":score.toString()));
        Object file_url = source.get("file_url");
        reportSegmentDto.setUrl(file_url==null?"":file_url.toString());
        Object segment = source.get("segment");
        reportSegmentDto.setSegment(segment==null?"":segment.toString());
        return reportSegmentDto;
    }

    private List<String> cutSentence(String sentence){
        if(StringUtils.isBlank(sentence))
            return Collections.EMPTY_LIST;
        AnalyzeRequestBuilder ikRequest = new AnalyzeRequestBuilder(elasticsearchTemplate.getClient(),
                AnalyzeAction.INSTANCE,ServiceConstants.INDEX_DOCUMENTS,sentence);
        ikRequest.setTokenizer(ServiceConstants.IK_SMART);
        List<AnalyzeResponse.AnalyzeToken> ikTokenList = ikRequest.execute().actionGet().getTokens();

        // 循环赋值
        List<String> searchTermList = new ArrayList<>();
        ikTokenList.forEach(ikToken -> { searchTermList.add(ikToken.getTerm()); });
        System.out.println(sentence+"===>分词为:"+searchTermList);
        return  searchTermList;
    }
}