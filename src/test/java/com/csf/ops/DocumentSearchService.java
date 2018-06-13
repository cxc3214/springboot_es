//package com.csf.ops;
//
//import com.csf.ops.search.constants.ServiceConstants;
//import com.csf.ops.search.dto.ReportDocDto;
//import com.csf.ops.search.dto.ReportSegmentDto;
//import com.csf.ops.search.util.ServiceUtils;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import org.apache.commons.collections4.CollectionUtils;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchType;
//import org.elasticsearch.common.text.Text;
//import org.elasticsearch.index.query.*;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.highlight.HighlightBuilder;
//import org.elasticsearch.search.highlight.HighlightField;
//import org.elasticsearch.search.sort.FieldSortBuilder;
//import org.elasticsearch.search.sort.SortBuilders;
//import org.elasticsearch.search.sort.SortOrder;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.domain.Page;
//import org.springframework.data.domain.PageImpl;
//import org.springframework.data.domain.Pageable;
//import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
//import org.springframework.data.elasticsearch.core.ResultsExtractor;
//import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
//import org.springframework.data.elasticsearch.core.query.SearchQuery;
//import org.springframework.stereotype.Service;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.stream.Collectors;
//import com.csf.ops.search.constants.ServiceConstants;
//import com.csf.ops.search.dto.ReportDocDto;
//import com.csf.ops.search.dto.ReportSegmentDto;
//import com.csf.ops.search.util.ServiceUtils;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.google.common.collect.Sets;
//import org.apache.commons.collections4.CollectionUtils;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchType;
//import org.elasticsearch.common.text.Text;
//import org.elasticsearch.index.query.*;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.SearchHits;
//import org.elasticsearch.search.aggregations.AggregationBuilders;
//import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
//import org.elasticsearch.search.aggregations.bucket.terms.Terms;
//import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
//import org.elasticsearch.search.aggregations.metrics.tophits.TopHits;
//import org.elasticsearch.search.aggregations.metrics.tophits.TopHitsBuilder;
//import org.elasticsearch.search.highlight.HighlightBuilder;
//import org.elasticsearch.search.highlight.HighlightField;
//import org.elasticsearch.search.sort.FieldSortBuilder;
//import org.elasticsearch.search.sort.SortBuilders;
//import org.elasticsearch.search.sort.SortOrder;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.domain.Page;
//import org.springframework.data.domain.PageImpl;
//import org.springframework.data.domain.Pageable;
//import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
//import org.springframework.data.elasticsearch.core.ResultsExtractor;
//import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
//import org.springframework.data.elasticsearch.core.query.SearchQuery;
//import org.springframework.stereotype.Service;
//
//import java.util.*;
//import java.util.stream.Collectors;
//
//@SuppressWarnings("Duplicates")
//public class DocumentSearchService {
//    @Autowired
//    ElasticsearchTemplate elasticsearchTemplate;
//
//    /*public Page<ReportDocDto> search2(String[] keyword, Pageable pageRequest) {
//        String keywords = String.join(",", keyword);
//        HashSet<String> searchKeyWords = Sets.newLinkedHashSet(Arrays.asList(keyword));
//        Map<String, List<Set<String>>> map = fetchComAlias(searchKeyWords);
//        Set<String> coms = map.keySet();
//
//        Set<String> allWords= (Set<String>) searchKeyWords.clone();
//        allWords.removeAll(coms);
//
//        List<org.elasticsearch.index.query.QueryBuilder> segmentQueryList=new ArrayList<>();
//        List<org.elasticsearch.index.query.QueryBuilder> titleQueryList=new ArrayList<>();
//        //公司
//        List<QueryBuilder> comTermQueryList =new ArrayList<>();
//        List<QueryBuilder> comQueryList_segment =new ArrayList<>();
////        List<QueryBuilder> comQueryList_title =new ArrayList<>();
//        for (String k : map.keySet()) {
//            List<Set<String>> sets = map.get(k);
//            Set<String> allSet= Sets.newHashSet();
//            sets.forEach(e->e.forEach(c->allSet.add(c)));
//            String join = String.join(",", allSet);
//            allSet.forEach(com-> comTermQueryList.add(QueryBuilders.termQuery("company_name",com)));
//            comQueryList_segment.add(QueryBuilders.matchQuery("segment",join));
////            comQueryList_title.add(QueryBuilders.matchQuery("title",join));
//        }
//
//        //属性
//        Map<String, List<Set<String>>> propMap = fetchPropAlias(allWords);
//        for (String k : propMap.keySet()) {
//            List<Set<String>> sets = propMap.get(k);
//            Set<String> allSet= Sets.newHashSet();
//            sets.forEach(e->e.forEach(c->allSet.add(c)));
//            String join = String.join(",", allSet);
//            segmentQueryList.add(QueryBuilders.matchQuery("segment",join));
////            titleQueryList.add(QueryBuilders.matchQuery("title",join));
//        }
//
//
//
//
//        TermsBuilder groupbyBuilder = AggregationBuilders.terms("groupby_id").field("id");
//        TopHitsBuilder topHis_data = AggregationBuilders.topHits("top_his_agg").setSize(1);
//        groupbyBuilder.subAggregation(topHis_data);
//        HighlightBuilder.Field company_name = new HighlightBuilder.Field("company_name");
//        topHis_data.addHighlightedField(company_name);
//
//        //com term
//        BoolQueryBuilder boolQueryBuilder_com_term = QueryBuilders.boolQuery();
//        comTermQueryList.forEach(q->boolQueryBuilder_com_term.should(q));
//        boolQueryBuilder_com_term.minimumNumberShouldMatch(1);
//        //com segment query
//        BoolQueryBuilder boolQueryBuilder_com_segment = QueryBuilders.boolQuery();
//        comQueryList_segment.forEach(q->boolQueryBuilder_com_segment.should(q));
//        boolQueryBuilder_com_segment.minimumNumberShouldMatch(1);
//
//        //com title query
////        BoolQueryBuilder boolQueryBuilder_com_title = QueryBuilders.boolQuery();
////        comQueryList_title.forEach(q->boolQueryBuilder_com_title.should(q));
////        boolQueryBuilder_com_title.minimumNumberShouldMatch(1);
//
//        //segment query
//        BoolQueryBuilder boolQueryBuilder_segment = QueryBuilders.boolQuery();
//        segmentQueryList.forEach(q->boolQueryBuilder_segment.should(q));
//        boolQueryBuilder_segment.minimumNumberShouldMatch(1);
//        //title query
////        BoolQueryBuilder boolQueryBuilder_title = QueryBuilders.boolQuery();
////        titleQueryList.forEach(q->boolQueryBuilder_title.should(q));
////        boolQueryBuilder_title.minimumNumberShouldMatch(1);
//
//
////        BoolQueryBuilder boolQueryBuilder_com_rs = QueryBuilders.boolQuery();
////        boolQueryBuilder_com_rs.must(boolQueryBuilder1).must(boolQueryBuilder2);
//
////        BoolQueryBuilder boolQueryBuilder_title_rs = QueryBuilders.boolQuery();
////        boolQueryBuilder_title_rs.must(boolQueryBuilder_com_title).must(boolQueryBuilder_title).must(boolQueryBuilder_com_term);
//
//        BoolQueryBuilder boolQueryBuilder_segment_rs = QueryBuilders.boolQuery();
//        boolQueryBuilder_segment_rs.must(boolQueryBuilder_com_segment).must(boolQueryBuilder_segment).must(boolQueryBuilder_com_term);
//
//
//
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
////        boolQueryBuilder.should(boolQueryBuilder_title_rs).should(boolQueryBuilder_segment_rs);
//        boolQueryBuilder.must(boolQueryBuilder_segment_rs);
//
//
//
//
//
//        SearchQuery searchQuery = new NativeSearchQueryBuilder()
////                .withQuery(contentQuery)
//                .withQuery(boolQueryBuilder)
//                .withSearchType(SearchType.DEFAULT)
//                .withIndices("documents").withTypes("segments")
//                .addAggregation(groupbyBuilder).withPageable(pageRequest)
//                .build();
//
//        int offset = pageRequest.getOffset();
//        int pageSize = pageRequest.getPageSize();
//
//        // when
//        Page<ReportDocDto> data = elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Page<ReportDocDto>>() {
//            @Override
//            public Page<ReportDocDto> extract(SearchResponse response) {
//                int count = 0;
//                int size = 1;
//                List<ReportDocDto> result = new ArrayList<ReportDocDto>();
//                StringTerms agg1 = response.getAggregations().get("groupby_id");
//                List<Terms.Bucket> buckets = agg1.getBuckets();
//                int totalCount = buckets.size();
//                for (Terms.Bucket bucket : buckets) {
//                    if (size > pageSize)
//                        break;
//                    TopHits topHits = bucket.getAggregations().get("top_his_agg");
//                    for (SearchHit hit : topHits.getHits()) {
//                        if (count++ < offset)
//                            continue;
//                        if (size++ > pageSize)
//                            break;
//                        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
//                        HighlightField title = highlightFields.get("company_name");
//                        Text[] fragments = title.getFragments();
//                        List<String> highlightList = ServiceUtils.regexMatch(fragments[0].string(), "<em>.+</em>");
//                        Map<String, Object> source = hit.getSource();
//                        float score = hit.getScore();
//                        source.put("score", score);
//                        source.put("keyword",highlightList);
//                        ReportDocDto reportDocDto =packingToDocumentDto(source);
//                        result.add(reportDocDto);
////                        try {
////                            ReportDocDto reportDocDto = MapUtils.convertMap(ReportDocDto.class, source);
////                            result.add(reportDocDto);
////                        } catch (Exception e) {
////                            e.printStackTrace();
////                        }
//                    }
//                }
//                Page<ReportDocDto> reportDocDtos = new PageImpl<ReportDocDto>(result, pageRequest, totalCount);
//                return reportDocDtos;
//            }
//        });
//        return data;
//    }*/
//
//    private Map<String, List<Set<String>>> fetchPropAlias(Set<String> allWords) {
//        Map<String,List<Set<String>>> result= Maps.newHashMap();
//        List<Set<String>> props = ServiceConstants.props;
//        for (String keyword : allWords) {
//            List<Set<String>> conts = props.stream().filter(e -> e.contains(keyword)).collect(Collectors.toList());
//            if(CollectionUtils.isNotEmpty(conts)){
//                result.put(keyword,conts);
//
//            } else{
//                ArrayList<Set<String>> list = Lists.newArrayList();
//                list.add(Sets.newHashSet(keyword));
//                result.put(keyword,list);
//            }
//        }
//        return result;
//    }
//
//    private Map<String,List<Set<String>>> fetchComAlias(Set<String> keywords) {
//        Map<String,List<Set<String>>> result= Maps.newHashMap();
//        List<Set<String>> coms = ServiceConstants.coms;
//        for (String keyword : keywords) {
//            List<Set<String>> conts = coms.stream().filter(e -> e.contains(keyword)).collect(Collectors.toList());
//            if(CollectionUtils.isNotEmpty(conts)){
//                result.put(keyword,conts);
//            }else{
//                ArrayList<Set<String>> list = Lists.newArrayList();
//                list.add(Sets.newHashSet(keyword));
//                result.put(keyword,list);
//            }
//        }
//        return result;
//    }
//
//
//    public Page<ReportSegmentDto> fetchDoc(String id, String[] keyWord, Pageable pageable){
//        String keywords = String.join(",", keyWord);
//        HighlightBuilder.Field title =new  HighlightBuilder.Field("title");
//        HighlightBuilder.Field segment = new HighlightBuilder.Field("segment");
//
//        TermQueryBuilder idQuery = QueryBuilders.termQuery("id", id);
//        MultiMatchQueryBuilder contQuery=new MultiMatchQueryBuilder(keywords,"title","segment").operator(MatchQueryBuilder.Operator.AND);
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(idQuery).must(contQuery);
//        FieldSortBuilder dtSortBuilder = SortBuilders.fieldSort("pub_date").order(SortOrder.DESC);
//        SearchQuery searchQuery = new NativeSearchQueryBuilder()
//                .withQuery(boolQueryBuilder)
//                .withSearchType(SearchType.DEFAULT).withSort(dtSortBuilder).withHighlightFields(title.preTags("<front>").postTags("</front>"),segment)
//                .withIndices("documents").withTypes("segments").withPageable(pageable)
//                .build();
//
//        return elasticsearchTemplate.query(searchQuery, new ResultsExtractor<Page<ReportSegmentDto>>() {
//            @Override
//            public Page<ReportSegmentDto> extract(SearchResponse searchResponse) {
//                List<ReportSegmentDto> result = new ArrayList<ReportSegmentDto>();
//                SearchHits hits = searchResponse.getHits();
//                long totalHits = hits.getTotalHits();
//                hits.forEach(hit -> {
//                    try {
//                        Float score = hit.getScore();
//                        Map<String, Object> source = hit.getSource();
//                        Map<String, HighlightField> highlightFields = hit.getHighlightFields();
//                        HighlightField segment1 = highlightFields.get("segment");
//                        HighlightField title1 = highlightFields.get("title");
//                        Long page = (Long) source.get("page");
//                        Text[] segment_fragment = segment1.getFragments();
//                        Text[] title_fragment = title1.getFragments();
//
//                        List<String> highlights=null;
//                        if(segment_fragment!=null && segment_fragment.length>0){
//                            highlights= ServiceUtils.regexMatch(title_fragment[0].string(),"<em>.+</em>");
//                        }
//                        source.put("keyword",highlights);
//                        source.put("score",score.isNaN()?1.0:score);
//                        result.add(packingToSegmentDto(source));
//                    } catch (Exception e1) {
//                        e1.printStackTrace();
//                    }
//                });
//                Page<ReportSegmentDto> reportSegmentDto = new PageImpl<ReportSegmentDto>(result, pageable, totalHits);
//                return reportSegmentDto;
//            }
//        });
//
//    }
//    private ReportDocDto packingToDocumentDto(Map<String, Object> source) {
//        ReportDocDto reportDocDto = new ReportDocDto();
//        Object file_url = source.get("file_url");
//        reportDocDto.setUrl(file_url==null?"":file_url.toString());
//        reportDocDto.setKeyword((List<String>)(source.get("keyword")));
//        Object score = source.get("score");
//        reportDocDto.setScore(Float.valueOf(score==null?"0":score.toString()));
//        Object file_type = source.get("file_type");
//        reportDocDto.setType(file_type==null?"":file_type.toString());
//        Object pub_date = source.get("pub_date");
//        reportDocDto.setPubtime(pub_date==null?"":pub_date.toString());
//        Object company_code = source.get("company_code");
//        reportDocDto.setCode(company_code==null?"":company_code.toString());
//        Object id = source.get("id");
//        reportDocDto.setId(id==null?"":id.toString());
//        Object company_name = source.get("company_name");
//        reportDocDto.setName(company_name==null?"":company_name.toString());
//        Object title = source.get("title");
//        reportDocDto.setTitle(title==null?"":title.toString());
//        return reportDocDto;
//    }
//    private ReportSegmentDto packingToSegmentDto(Map<String, Object> source) {
//        ReportSegmentDto reportSegmentDto = new ReportSegmentDto();
//        Object company_code = source.get("company_code");
//        reportSegmentDto.setCode(company_code==null?"":company_code.toString());
//        Object file_type = source.get("file_type");
//        reportSegmentDto.setType(file_type==null?"":file_type.toString());
//        Object title = source.get("title");
//        reportSegmentDto.setTitle(title==null?"":title.toString());
////        reportSegmentDto.setIdx(source.get(""));
//        reportSegmentDto.setKeyword((List<String>)(source.get("keyword")));
//        Object page = source.get("page");
//        reportSegmentDto.setPage(Integer.valueOf(page==null?"0":page.toString()));
//        Object company_name = source.get("company_name");
//        reportSegmentDto.setName(company_name==null?"":company_name.toString());
//        Object pub_date = source.get("pub_date");
//        reportSegmentDto.setPubtime(pub_date==null?"":pub_date.toString());
//        Object score = source.get("score");
//        reportSegmentDto.setScore(Float.valueOf(score==null?"":score.toString()));
//        Object file_url = source.get("file_url");
//        reportSegmentDto.setUrl(file_url==null?"":file_url.toString());
//        Object segment = source.get("segment");
//        reportSegmentDto.setSegment(segment==null?"":segment.toString());
//        return reportSegmentDto;
//    }
//}