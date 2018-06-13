package com.csf.ops.search.util;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServiceUtils {
    public static Set<String> regexMatch(String word, String regex){
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(word);
        Set<String> result=new LinkedHashSet<>();
        while(m.find()){
            String group = m.group();
            group=group.substring(4,group.length()-5);
            result.add(group);
        }

        return result;
    }

    public static void main(String[] args) {
        Set<String> list = regexMatch("<em>工商银行</em>hello<em>中国银行</em>", "<em>((?![(<em>)|(</em>)]).)+</em>");
        System.out.println(list);
    }
}