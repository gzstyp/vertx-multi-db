package com.fwtai.tool;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

import java.util.List;

/**
 * lambda表达式
 * @作者 田应平
 * @版本 v1.0
 * @创建时间 2020-09-30 9:50
 * @QQ号码 444141300
 * @Email service@dwlai.com
 * @官网 http://www.fwtai.com
*/
public final class ToolLambda{

  public static Future<JsonObject> getConfig(final ConfigRetriever retriever){
    final Promise<JsonObject> promise = Promise.promise();
    retriever.getConfig(asyncResult->{
      if(asyncResult.succeeded()){
        promise.complete(asyncResult.result());//重点,固定写法
      }else{
        promise.fail(asyncResult.cause());//重点,固定写法
      }
    });
    return promise.future();//重点,固定写法
  }

  // ①获取数据库连接,通过链式调用;异步+响应式的链式调用示例,有且只有包含 Handler + AsyncResult 才能封装成链式调用
  public static Future<SqlConnection> getMySqlConn(final MySQLPool client){
    final Promise<SqlConnection> promise = Promise.promise();
    client.getConnection(asyncResult ->{
      if(asyncResult.succeeded()){
        promise.complete(asyncResult.result());//重点,固定写法
      }else{
        promise.fail(asyncResult.cause());//重点,固定写法
      }
    });
    return promise.future();//重点,固定写法
  }

  // ②用获取到的连接查询数据库
  public static Future<RowSet<Row>> getRows(final SqlConnection connection,final String sql,final List<Object> params){
    final Promise<RowSet<Row>> promise = Promise.promise();
    connection.preparedQuery(sql).execute(Tuple.wrap(params),handler ->{
      if(handler.succeeded()){
        promise.complete(handler.result());//重点,固定写法
      }else{
        promise.fail(handler.cause());//重点,固定写法
      }
    });
    return promise.future();//重点,固定写法
  }
}