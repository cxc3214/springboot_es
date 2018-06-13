package com.csf.ops.search.constants;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;

public class ServiceConstants {


    public static final String REGEX_KEYWORD="<em>((?![(<em>)|(</em>)]).)+</em>";
    public static final String INDEX_DOCUMENTS = "documents";
    public static final String IK_MAX_WORD = "ik_max_word";
    public static final String IK_SMART = "ik_smart";
    public static final String IK_SYNO = "ik_syno";
    public static final String TYPE_SEGMENT = "segments";
    public static  Map<String,Set<String>> coms;
    public static Map<String,Set<String>> props;

    public static void initData(String path){
//        String p = Thread.currentThread().getContextClassLoader().getResource("/../../../").getPath();
//        System.out.println("============="+p);
//        String path = ServiceConstants.class.getClassLoader().getResource("file").getFile();
        try {
//            InputStream in = ServiceConstants.class.getClassLoader().getResourceAsStream("file/com.txt");
//            List<String> lines = IOUtils.readLines(in);
            List<String> lines = FileUtils.readLines(new File(path+"/com.txt"));
            coms = lines.stream().map(line -> line.split("#")).collect(Collectors.groupingBy(k->k[0],Collectors.mapping(k->k[1],Collectors.toSet())));
            coms.forEach((k,v)->v.add(k));
        } catch (IOException e) {
            e.printStackTrace();
        }
//        Set<String> set= Sets.newHashSet("工商银行","工行","中国工商银行","工商");
//        coms.put("工商银行",set);

//        props= Maps.newHashMap();
//        Set<String> set2= Sets.newHashSet("业绩","销售额","毛利润");
//        props.put("业绩",set2);

        try {
            props= Maps.newHashMap();
            List<String> lines = FileUtils.readLines(new File(path+"/event.txt"));
//            InputStream in = ServiceConstants.class.getClassLoader().getResourceAsStream("file/event.txt");
//            List<String> lines = IOUtils.readLines(in);
            Map<String,Set<String>> props_tmp = lines.stream().map(line -> line.split("#")).collect(Collectors.groupingBy(k->k[0],Collectors.mapping(k->k[1],Collectors.toSet())));
            props_tmp.forEach((k,v)->v.add(k));
            props.putAll(props_tmp);

            lines = FileUtils.readLines(new File(path+"/finace.txt"));
//            in = ServiceConstants.class.getClassLoader().getResourceAsStream("file/finace.txt");
//            lines = IOUtils.readLines(in);
            props_tmp = lines.stream().map(line -> line.split("#")).collect(Collectors.groupingBy(k->k[0],Collectors.mapping(k->k[1],Collectors.toSet())));
            props_tmp.forEach((k,v)->v.add(k));
            props.putAll(props_tmp);

            lines = FileUtils.readLines(new File(path+"/normal.txt"));
//            in = ServiceConstants.class.getClassLoader().getResourceAsStream("file/normal.txt");
//            lines = IOUtils.readLines(in);
            props_tmp = lines.stream().map(line -> line.split("#")).collect(Collectors.groupingBy(k->k[0],Collectors.mapping(k->k[1],Collectors.toSet())));
            props_tmp.forEach((k,v)->v.add(k));
            props.putAll(props_tmp);

            lines = FileUtils.readLines(new File(path+"/prod.txt"));
//            in = ServiceConstants.class.getClassLoader().getResourceAsStream("file/prod.txt");
//            lines = IOUtils.readLines(in);
            props_tmp = lines.stream().map(line -> line.split("#")).collect(Collectors.groupingBy(k->k[0],Collectors.mapping(k->k[1],Collectors.toSet())));
            props_tmp.forEach((k,v)->v.add(k));
            props.putAll(props_tmp);

            lines = FileUtils.readLines(new File(path+"/work.txt"));
//            in = ServiceConstants.class.getClassLoader().getResourceAsStream("file/work.txt");
//            lines = IOUtils.readLines(in);;
            props_tmp = lines.stream().map(line -> line.split("#")).collect(Collectors.groupingBy(k->k[0],Collectors.mapping(k->k[1],Collectors.toSet())));
            props_tmp.forEach((k,v)->v.add(k));
            props.putAll(props_tmp);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}