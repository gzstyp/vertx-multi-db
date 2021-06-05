package com.fwtai.tool;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class ToolClient{

  private static final String key_data = "data";
  private static final String key_msg = "msg";
  private static final String key_code = "code";

  public static String createJson(final int code,final String msg){
    final JsonObject json = new JsonObject();
    json.put(key_code,code);
    json.put(key_msg,msg);
    return json.encode();
  }

  public static String jsonFailure(){
    return createJson(199,"操作失败");
  }

  public static String jsonFailure(final String msg){
    return createJson(199,msg);
  }

  public static String jsonSucceed(){
    return createJson(200,"操作成功");
  }

  public static String jsonSucceed(final String msg){
    return createJson(200,msg);
  }

  public static String jsonEmpty(){
    return createJson(201,"暂无数据");
  }

  public static String jsonParams(){
    return createJson(202,"请求参数不完整");
  }

  public static String jsonError(){
    return createJson(204,"系统出现错误");
  }

  public static String jsonPermission(){
    return createJson(401,"没有操作权限");
  }

  public static String queryJson(final Object object){
    if(object == null || object.toString().trim().length() == 0)
      return jsonEmpty();
    if(object instanceof Map<?,?>){
      final Map<?,?> map = (Map<?,?>) object;
      if(map.size() <= 0){
        return jsonEmpty();
      }
    }
    if(object instanceof JsonObject){
      final JsonObject jsonObject = (JsonObject) object;
      if(jsonObject.isEmpty()){
        return jsonEmpty();
      }
    }
    if(object instanceof List<?>){
      final List<?> list = (List<?>) object;
      if(list.size() <= 0){
        return jsonEmpty();
      }
    }
    final JsonObject json = new JsonObject();
    json.put(key_code,200);
    json.put(key_msg,"操作成功");
    json.put(key_data,object);
    return json.encode();
  }

  public static String executeRows(final int rows){
    final JsonObject json = new JsonObject();
    if(rows > 0){
      json.put(key_code,200);
      json.put(key_msg,"操作成功");
      json.put(key_data,rows);
      return json.encode();
    }else{
      return jsonFailure();
    }
  }

  public static HttpServerResponse getResponse(final RoutingContext context){
    return context.response().putHeader("Server","vert.x").putHeader("Cache-Control","no-cache").putHeader("content-type","text/html;charset=utf-8");
  }

  public static HttpServerResponse getResponse(final HttpServerRequest request){
    return request.response().putHeader("Cache-Control","no-cache").putHeader("content-type","text/html;charset=utf-8");
  }

  /**响应json数据:第二个参数是json格式数据*/
  public static void responseJson(final RoutingContext context,final String payload){
    getResponse(context).end(payload);
  }

  /**响应json数据:code=202;msg=暂无数据*/
  public static void responseEmpty(final RoutingContext context){
    getResponse(context).end(jsonEmpty());
  }

  /**响应json数据:code=204;msg=系统出现错误*/
  public static void responseError(final RoutingContext context){
    getResponse(context).end(jsonError());
  }

  /**响应json数据:code=200;msg=操作成功*/
  public static void responseSucceed(final RoutingContext context){
    getResponse(context).end(jsonSucceed());
  }

  /**响应json数据:code=200;msg=指定的msg*/
  public static void responseSucceed(final RoutingContext context,final String msg){
    getResponse(context).end(jsonSucceed(msg));
  }

  /**响应json数据:code=199;msg=操作失败*/
  public static void responseFailure(final RoutingContext context){
    getResponse(context).end(jsonFailure());
  }

  /**响应json数据:code=199;msg=指定的msg*/
  public static void responseFailure(final RoutingContext context,final String msg){
    getResponse(context).end(jsonFailure(msg));
  }

  public static String validateField(final HttpServerRequest request,final String... fields){
    boolean bl = false;
    for(int i = 0; i < fields.length;i++){
      final String value = request.getParam(fields[i]);
      if(value == null || value.trim().length() == 0){
        bl = true;
        break;
      }
    }
    if(bl) return jsonParams();
    return null;
  }

  /**验证请求参数是否完整,先调用 getParams() 再调用本方法
   final String validateField = ToolClient.validateField(params,"deviceFlag","volume");
   if(validateField != null){
     ToolClient.responseJson(context,validateField);
     return;
   }
  */
  public static String validateField(final HashMap<String,String> params,final String... fields){
    boolean bl = false;
    if(params == null || params.isEmpty()) return jsonParams();
    for(int x = 0; x < fields.length; x++){
      final String key = fields[x];
      final String value = params.get(key);
      if(value == null || value.isEmpty()){
        bl = true;
        break;
      }
    }
    if(bl) return jsonParams();
    return null;
  }

  /**获取表单请求参数*/
  public static HashMap<String,String> getParams(final RoutingContext context){
    final HashMap<String,String> result = new HashMap<>();
    final List<Map.Entry<String,String>> list = context.queryParams().entries();
    for(int i = 0; i < list.size(); i++){
      final Map.Entry<String,String> entry = list.get(i);
      final String value = entry.getValue();
      if(value != null && !value.isEmpty()){
        result.put(entry.getKey(),entry.getValue());
      }
    }
    return result;
  }

  /**多线程下生成32位唯一的字符串*/
  public static String getIdsChar32(){
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    return new UUID(random.nextInt(),random.nextInt()).toString().replaceAll("-","");
  }
}