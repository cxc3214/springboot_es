package com.csf.ops.search.web;

import com.aug3.sys.rs.response.RespObj;
import com.csf.ops.search.service.DocumentSearchService;
import com.csf.ops.search.util.IKAnalyzerUtils;
import com.csf.ops.search.web.base.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.data.web.PageableDefault;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/search")
public class DocumentSearchController extends BaseController {

    @Autowired
    DocumentSearchService documentSearchService;

    /**
     * 搜索公司文档列表
     * @param keyword
     * @param pageRequest
     * @return
     */
    @RequestMapping(value = "/company_list")
    public RespObj search(String keyword, @PageableDefault Pageable pageRequest){
        return build(documentSearchService.search(keyword,pageRequest));
    }

    /**
     * 搜索文档
     * @param id
     * @param keyword
     * @param pageRequest
     * @return
     */
    @RequestMapping(value = "/company_document")
    public RespObj segment(String id,String keyword, @PageableDefault Pageable pageRequest){
        return build(documentSearchService.fetchDoc(id,keyword,pageRequest));
    }
}