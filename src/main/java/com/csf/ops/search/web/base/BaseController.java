package com.csf.ops.search.web.base;

import com.aug3.sys.rs.response.RespObj;
import com.aug3.sys.rs.response.RespType;

/**
 * Created by jun.wan on 2017/12/14.
 *
 */
public class BaseController {

    public static RespObj build(){

        return build(true);
    }

    protected static RespObj paramError(){
        return build(RespType.BAD_REQUEST,"参数错误");
    }

    public static RespObj build(Object data){

        RespObj obj = new RespObj();
        obj.setType(RespType.SUCCESS.name());
        obj.setCode(RespType.SUCCESS.getCode());
        obj.setMessage(data);
        return obj;
    }

    public static RespObj build(RespType type, Object data){

        RespObj obj = new RespObj();
        obj.setType(type.name());
        obj.setCode(type.getCode());
        obj.setMessage(data);
        return obj;
    }

    public static RespObj buildServerErrorResp(String message) {
        RespObj respObj = new RespObj();
        respObj.setCode(RespType.INTERNAL_SERVER_ERROR.getCode());
        respObj.setType(RespType.INTERNAL_SERVER_ERROR.name());
        respObj.setMessage(message);
        return respObj;
    }

}
