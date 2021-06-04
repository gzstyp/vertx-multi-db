package com.fwtai.dao;

import com.fwtai.callback.QueryResultMap;
import com.fwtai.tool.ToolClient;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class DaoHandle{

  final InternalLogger logger = Log4JLoggerFactory.getInstance(getClass());

  private final AtomicInteger count = new AtomicInteger(0);

  private final int dbTotal = 2;//两个读库

  private final DBRead0 dbRead0;
  private final DBRead1 dbRead1;
  private final DBWrite dbWrite;

  public DaoHandle(final Vertx vertx){
    this.dbRead0 = new DBRead0(vertx);
    this.dbRead1 = new DBRead1(vertx);
    this.dbWrite = new DBWrite(vertx);
  }

  public MySQLPool getPool(){
    final int total = count.getAndAdd(1);
    final int key = total % dbTotal;
    if(key == 0){
      return this.dbRead0.getClient();
    }else{
      return this.dbRead1.getClient();
    }
  }

  //todo 推荐,有参数 new ToolMySQL(vertx).queryList();若没有参数的话,要创建 new ArrayList<Object>(0) 作为第3个参数
  public final void queryList(final RoutingContext context,final String sql,final List<Object> params){
    this.getPool().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final ArrayList<JsonObject> list = new ArrayList<>();
            final RowSet<Row> rowSet = rows.result();
            final List<String> columns = rowSet.columnsNames();
            rowSet.forEach((item) ->{
              final JsonObject jsonObject = new JsonObject();
              for(int i = 0; i < columns.size(); i++){
                final String column = columns.get(i);
                jsonObject.put(column,item.getValue(column));
              }
              list.add(jsonObject);
            });
            //操作数据库成功
            ToolClient.responseJson(context,ToolClient.queryJson(list));
          }else{
            logger.error("queryList()出现异常,连接数据库失败:"+sql);
            //操作数据库失败
            final String json = ToolClient.createJson(199,"连接数据库失败");
            ToolClient.responseJson(context,json);
          }
        });
      }
    });
  }

  //todo 推荐,有参数 new ToolMySQL(vertx).queryList();若没有参数的话,要创建 new ArrayList<Object>(0) 作为第3个参数
  public final void queryMap(final RoutingContext context,final String sql,final List<Object> params){
    this.getPool().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final JsonObject jsonObject = new JsonObject();
            final RowSet<Row> rowSet = rows.result();
            final List<String> columns = rowSet.columnsNames();
            rowSet.forEach((item) ->{
              for(int i = 0; i < columns.size();i++){
                final String column = columns.get(i);
                jsonObject.put(column,item.getValue(column));
              }
            });
            ToolClient.responseJson(context,ToolClient.queryJson(jsonObject));
          }else{
            logger.error("queryMap()出现异常,连接数据库失败:"+sql);
            final String json = ToolClient.createJson(199,"连接数据库失败");
            ToolClient.responseJson(context,json);
          }
        });
      }
    });
  }

  public final void queryMap(final String sql,final List<Object> params,final QueryResultMap queryResultMap){
    this.getPool().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final JsonObject jsonObject = new JsonObject();
            final RowSet<Row> rowSet = rows.result();
            final List<String> columns = rowSet.columnsNames();
            rowSet.forEach((item) ->{
              for(int i = 0; i < columns.size();i++){
                final String column = columns.get(i);
                jsonObject.put(column,item.getValue(column));
              }
            });
            queryResultMap.succeed(jsonObject);
          }else{
            queryResultMap.failure(rows.cause());
          }
        });
      }
    });
  }

  public final void execute(final RoutingContext context,final String sql,final List<Object> params){
    this.dbWrite.getClient().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final RowSet<Row> rowSet = rows.result();
            final int count = rowSet.rowCount();
            ToolClient.responseJson(context,ToolClient.executeRows(count));
          }else{
            failure(context,rows.cause());
          }
        });
      }
    });
  }

  protected void failure(final RoutingContext context,final Throwable throwable){
    final String message = throwable.getMessage();
    if(message.contains("cannot be null")){
      ToolClient.responseJson(context,ToolClient.jsonParams());
    }else if(message.contains("Duplicate entry")){
      ToolClient.responseJson(context,ToolClient.createJson(199,"数据已存在"));
    }else{
      ToolClient.responseJson(context,ToolClient.jsonFailure());
    }
  }

  //用法,daoHandle.execute(sql,params).onSuccess(handler->{}).onFailure(throwable->{});
  /* 示例:
    daoHandle.execute(sqlAdd,paramsAdd).onSuccess(rows->{
      final RowSet<Row> rowSet = rows.value();
      final int count = rowSet.rowCount();
      System.out.println("count->"+count);
      ToolClient.responseJson(context,ToolClient.executeRows(count));
    }).onFailure(err->{
      ToolClient.responseJson(context,ToolClient.createJson(199,"连接数据库失败"));
    });
  */
  public final Future<RowSet<Row>> execute(final String sql,final List<Object> params){
    final Promise<RowSet<Row>> promise = Promise.promise();
    this.dbWrite.getClient().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            promise.complete(rows.result());//重点,固定写法
          }else{
            promise.fail(rows.cause());//重点,固定写法
          }
        });
      }
    });
    return promise.future();//重点,固定写法
  }
}