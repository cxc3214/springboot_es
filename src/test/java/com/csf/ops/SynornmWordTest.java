package com.csf.ops;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SynornmWordTest {

    public static void main(String[] args) throws IOException {
        List<String> lines=null;
        //公司
//         lines= FileUtils.readLines(new File("E:\\HYB_WORK_PACE\\idea_workpace_new\\doc_search\\src\\test\\java\\file\\公司"));
//        lines.stream().map(line->line.split(",")).forEach(e-> System.out.println(e[1]+"#"+e[2]));
//        lines.stream().map(line->line.split(",")).map(e->e[1]).collect(Collectors.toSet()).forEach(System.out::println);
//        //事件
        lines = FileUtils.readLines(new File("E:\\HYB_WORK_PACE\\idea_workpace_new\\doc_search\\src\\test\\java\\file\\事件"));
//        lines.stream().map(line->line.split(",")).forEach(e-> System.out.println(e[1]+"#"+e[2]));
//        lines.stream().map(line->line.split(",")).map(e->e[1]).collect(Collectors.toSet()).forEach(System.out::println);
//        //职位
//        lines = FileUtils.readLines(new File("E:\\HYB_WORK_PACE\\idea_workpace_new\\doc_search\\src\\test\\java\\file\\职位"));
//        lines.stream().map(line->line.split(",")).forEach(e-> System.out.println(e[1]+"#"+e[2]));
//        lines.stream().map(line->line.split(",")).map(e->e[1]).collect(Collectors.toSet()).forEach(System.out::println);
//        //行业
//        lines = FileUtils.readLines(new File("E:\\HYB_WORK_PACE\\idea_workpace_new\\doc_search\\src\\test\\java\\file\\行业"));
//        lines.stream().map(line->line.split(",")).forEach(e-> System.out.println(e[1]+"#"+e[2]));
//        lines.stream().map(line->line.split(",")).map(e->e[1]).collect(Collectors.toSet()).forEach(System.out::println);
//
//        //财务
//        lines = FileUtils.readLines(new File("E:\\HYB_WORK_PACE\\idea_workpace_new\\doc_search\\src\\test\\java\\file\\财务"));
//        lines.stream().map(line->line.split(",")).forEach(e-> System.out.println(e[1]+"#"+e[2]));
//        lines.stream().map(line->line.split(",")).map(e->e[1]).collect(Collectors.toSet()).forEach(System.out::println);
//
//        //普通
//        lines = FileUtils.readLines(new File("E:\\HYB_WORK_PACE\\idea_workpace_new\\doc_search\\src\\test\\java\\file\\普通"));
//        lines.stream().map(line->line.split(",")).forEach(e-> System.out.println(e[1]+"#"+e[2]));
        lines.stream().map(line->line.split(",")).flatMap(e-> Stream.of(e[1],e[2])).collect(Collectors.toSet()).forEach(System.out::println);


    }
}