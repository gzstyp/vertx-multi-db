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
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.templates.SqlTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

//todo 事务: https://vertx-china.gitee.io/docs/vertx-mysql-client/java/#_using_transactions
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

  public MySQLPool getQuery(){
    final int total = count.getAndAdd(1);
    final int key = total % dbTotal;
    if(key == 0){
      return this.dbRead0.getClient();
    }else{
      return this.dbRead1.getClient();
    }
  }

  //todo 推荐,daoHandle.queryList(context,sql,new ArrayList<Object>(0));若没有参数的话,要创建 new ArrayList<Object>(0) 作为第3个参数
  public final void queryList(final RoutingContext context,final String sql,final List<Object> params){
    this.getQuery().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final ArrayList<JsonObject> list = getRowList(rows.result());
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

  //推荐 daoHandle.queryList(sql,new ArrayList<>(0)).onSuccess(list->{}).onFailure(err->{});若有参数的话,要创建 new ArrayList<Object>(0)即可
  public final Future<ArrayList<JsonObject>> queryList(final String sql,final List<Object> params){
    final Promise<ArrayList<JsonObject>> promise = Promise.promise();
    this.getQuery().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            promise.complete(getRowList(rows.result()));
          }else{
            promise.fail(rows.cause());
          }
        });
      }
    });
    return promise.future();
  }

  //推荐 daoHandle.queryMap(sql,new ArrayList<>(0)).onSuccess(map->{}).onFailure(err->{});若有参数的话,要创建 new ArrayList<Object>(0)即可
  public final Future<JsonObject> queryMap(final String sql,final List<Object> params){
    final Promise<JsonObject> promise = Promise.promise();
    this.getQuery().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final JsonObject jsonObject = getRowMap(rows.result());
            promise.complete(jsonObject);
          }else{
            promise.fail(rows.cause());
          }
        });
      }
    });
    return promise.future();
  }

  protected final ArrayList<JsonObject> getRowList(final RowSet<Row> rowSet){
    final ArrayList<JsonObject> list = new ArrayList<>();
    final List<String> columns = rowSet.columnsNames();
    rowSet.forEach((item) ->{
      final JsonObject jsonObject = new JsonObject();
      for(int i = 0; i < columns.size(); i++){
        final String column = columns.get(i);
        jsonObject.put(column,item.getValue(column));
      }
      list.add(jsonObject);
    });
    return list;
  }

  //todo 推荐,有参数 new ToolMySQL(vertx).queryList();若没有参数的话,要创建 new ArrayList<Object>(0) 作为第3个参数
  public final void queryMap(final RoutingContext context,final String sql,final List<Object> params){
    this.getQuery().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            final JsonObject jsonObject = getRowMap(rows.result());
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

  protected final JsonObject getRowMap(final RowSet<Row> rowSet){
    final JsonObject jsonObject = new JsonObject();
    final List<String> columns = rowSet.columnsNames();
    rowSet.forEach((item) ->{
      for(int i = 0; i < columns.size();i++){
        final String column = columns.get(i);
        jsonObject.put(column,item.getValue(column));
      }
    });
    return jsonObject;
  }

  public final void queryMap(final String sql,final List<Object> params,final QueryResultMap queryResultMap){
    this.getQuery().getConnection((result) ->{
      if(result.succeeded()){
        final SqlConnection conn = result.result();
        conn.preparedQuery(sql).execute(Tuple.wrap(params),rows ->{
          conn.close();//推荐写在第1行,防止忘记释放资源
          if(rows.succeeded()){
            queryResultMap.succeed(getRowMap(rows.result()));
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

  protected void getSql(final String table,final ArrayList<String> fields,final String where,final String order,final Integer limit){
    final StringBuilder sb = new StringBuilder("select ");
    for(int i = 0; i < fields.size(); i++){
      final String field = fields.get(i);
      if(sb.length() > 7){
        sb.append(",").append(field);
      }else{
        sb.append(field);
      }
    }
    sb.append(" from ").append(table);
    if(where != null){
      sb.append(" where ").append(where);
    }
    if(order != null){
      sb.append(" order by ").append(order);
    }
    if(limit != null){
      sb.append(" LIMIT ").append(limit);
    }
  }

  /**
   * 查询操作,待验证
   * @param parameters --> SELECT name FROM users WHERE id=#{id}
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021/6/9 19:59
  */
  public final Future<RowSet<Row>> query(final String sql,final HashMap<String,Object> parameters){
    final Promise<RowSet<Row>> promise = Promise.promise();
    final Future<RowSet<Row>> execute = SqlTemplate.forQuery(this.getQuery(),sql).execute(parameters);
    execute.onSuccess(handler->{
      promise.complete(execute.result());
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }

  /**
   * 新增|更新|删除,待验证
   * @param parameters --> INSERT INTO users VALUES (#{id},#{name})
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021/6/9 19:58
  */
  public final Future<Integer> execute(final String sql,final HashMap<String,Object> parameters){
    final Promise<Integer> promise = Promise.promise();
    final Future<SqlResult<Void>> execute = SqlTemplate.forUpdate(this.dbWrite.getClient(),sql).execute(parameters);
    execute.onSuccess(handler->{
      promise.complete(handler.rowCount());
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }
}