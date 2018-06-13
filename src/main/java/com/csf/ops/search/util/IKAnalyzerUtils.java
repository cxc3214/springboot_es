package com.csf.ops.search.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequestBuilder;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IKAnalyzerUtils {

    public static List<String> analyze(String content){
        /*Analyzer analyzer = new IKAnalyzer(true);
        TokenStream ts = null;
        try {
            ts = analyzer.tokenStream("", new StringReader(content));
            CharTermAttribute term = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            List<String> vocabularies = new ArrayList<>();
            while (ts.incrementToken()) {
                vocabularies.add(term.toString());
            }
            ts.end();
            return vocabularies;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ts != null) {
                try {
                    ts.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }*/
        return Collections.EMPTY_LIST;
    }

//    public static String jieBaanalyze(String content){
//        JiebaSegmenter segmenter = new JiebaSegmenter();
//        List<String> stringList = segmenter.sentenceProcess(content);
//        System.out.println(stringList);
//        System.out.println("--------------------------------------------------------");
//        return segmenter.process(content, JiebaSegmenter.SegMode.SEARCH).toString();
//    }

    public void test(){
//        AnalyzeRequestBuilder ikRequest = new AnalyzeRequestBuilder(elasticsearchTemplate.getClient(),
//                AnalyzeAction.INSTANCE,"documents","我是中华人民共和国的合法公民");
//        ikRequest.setTokenizer("ik");
//        List<AnalyzeResponse.AnalyzeToken> ikTokenList = ikRequest.execute().actionGet().getTokens();
//
//        // 循环赋值
//        List<String> searchTermList = new ArrayList<>();
//        ikTokenList.forEach(ikToken -> { searchTermList.add(ikToken.getTerm()); });
//
//        System.out.println(searchTermList);
    }

    public static void main(String[] args) {
//        String s = jieBaanalyze("最新的快照版本去除词性标注，也希望有更好的 Pull Request 可以提供该功能");
//        System.out.println(s);
        List<String> analyze = analyze("最新的快照版本去除词性标注，也希望有更好的 Pull Request 可以提供该功能");
        System.out.println(analyze);
    }

}