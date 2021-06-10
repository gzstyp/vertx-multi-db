package com.fwtai.dao;

import com.fwtai.callback.QueryResultMap;
import com.fwtai.tool.ToolClient;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import io.vertx.core.CompositeFuture;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  protected final MySQLPool getQuery(){
    final int total = count.getAndAdd(1);
    final int key = total % dbTotal;
    if(key == 0){
      return this.dbRead0.getClient();
    }else{
      return this.dbRead1.getClient();
    }
  }

  /**用于处理事务*/
  public MySQLPool getClient(){
    return this.dbWrite.getClient();
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

  protected final JsonObject getRowMap(final RowSet<Row> rowSet){
    final JsonObject jsonObject = new JsonObject();
    final List<String> columns = rowSet.columnsNames();
    rowSet.forEach((item) ->{
      for(int i = 0; i < columns.size();i++){
        final String column = columns.get(i);
        final Object value = item.getValue(column);
        if(value != null){
          jsonObject.put(column,value);
        }
      }
    });
    return jsonObject;
  }

  //todo 此处的JsonObject不能用上面的方法getRowMap()!!!
  protected final ArrayList<JsonObject> getRowList(final RowSet<Row> rowSet){
    final ArrayList<JsonObject> list = new ArrayList<>();
    final List<String> columns = rowSet.columnsNames();
    rowSet.forEach((item) ->{
      final JsonObject jsonObject = new JsonObject();
      for(int i = 0; i < columns.size(); i++){
        final String column = columns.get(i);
        final Object value = item.getValue(column);
        if(value != null){
          jsonObject.put(column,value);
        }
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

  /**
   * 基于SqlTemplate新增|更新|删除,已处理错误时的提示,仅需处理 daoHandle(sql,params,context).onSuccess(json->{});
   * @param sql --> INSERT INTO users VALUES (#{id},#{name})
   * @param params --> map.put("id",1024);若没有参数时传入 new HashMap<>(0)
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021/6/9 19:58
  */
  public final Future<Integer> execute(final String sql,final Map<String,Object> params,final RoutingContext context){
    final Promise<Integer> promise = Promise.promise();
    final Future<SqlResult<Void>> execute = SqlTemplate.forUpdate(this.dbWrite.getClient(),sql).execute(params);
    execute.onSuccess(handler->{
      promise.complete(handler.rowCount());
    }).onFailure(err->{
      failure(context,err);
    });
    return promise.future();
  }

  /**
   * todo 基于SqlTemplate查询操作,推荐!!!用法 https://vertx.io/docs/vertx-sql-client-templates/java/#_getting_started
   * @param sql --> SELECT name FROM users WHERE id=#{id}
   * @param params map.put("id",1024),若没有参数时传入 new HashMap<>(0)
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021/6/9 19:59
  */
  public final Future<ArrayList<JsonObject>> queryList(final String sql,final Map<String,Object> params){
    final Promise<ArrayList<JsonObject>> promise = Promise.promise();
    final Future<RowSet<Row>> execute = SqlTemplate.forQuery(this.getQuery(),sql).execute(params);
    execute.onSuccess(rows->{
      promise.complete(getRowList(rows.value()));
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }

  /**
   * todo 基于SqlTemplate查询操作,推荐
   * @param sql --> SELECT name FROM users WHERE id=#{id}
   * @param params map.put("id",1024),若没有参数时传入 new HashMap<>(0)
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021年6月9日 23:15:43
  */
  public final Future<JsonObject> queryMap(final String sql,final Map<String,Object> params){
    final Promise<JsonObject> promise = Promise.promise();
    final Future<RowSet<Row>> execute = SqlTemplate.forQuery(this.getQuery(),sql).execute(params);
    execute.onSuccess(rows->{
      promise.complete(getRowMap(rows.value()));
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }

  public final Future<Integer> queryInteger(final String sql,final Map<String,Object> params){
    final Promise<Integer> promise = Promise.promise();
    final Future<RowSet<Row>> execute = SqlTemplate.forQuery(this.getQuery(),sql).execute(params);
    execute.onSuccess(rows->{
      final JsonObject jsonObject = getRowMap(rows.value());
      final Set<String> keys = jsonObject.fieldNames();
      Integer record = null;
      for(final String key : keys){
        record = jsonObject.getInteger(key);
      }
      promise.complete(record);
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }

  public final Future<String> queryString(final String sql,final Map<String,Object> params){
    final Promise<String> promise = Promise.promise();
    final Future<RowSet<Row>> execute = SqlTemplate.forQuery(this.getQuery(),sql).execute(params);
    execute.onSuccess(rows->{
      final JsonObject jsonObject = getRowMap(rows.value());
      final Set<String> keys = jsonObject.fieldNames();
      String record = null;
      for(final String key : keys){
        record = jsonObject.getString(key);
      }
      promise.complete(record);
    }).onFailure(err->{
      promise.fail(err.getCause());
      logger.error(err);
    });
    return promise.future();
  }

  /**
   * todo 基于SqlTemplate新增|更新|删除,推荐!
   * @param sql --> INSERT INTO users VALUES (#{id},#{name})
   * @param params --> map.put("id",1024);若没有参数时传入 new HashMap<>(0)
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021/6/9 19:58
  */
  public final Future<Integer> execute(final String sql,final Map<String,Object> params){
    final Promise<Integer> promise = Promise.promise();
    final Future<SqlResult<Void>> execute = SqlTemplate.forUpdate(this.dbWrite.getClient(),sql).execute(params);
    execute.onSuccess(handler->{
      promise.complete(handler.rowCount());
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }

  /**
   * 分页功能
   * @param sqlListData 查询数据sql
   * @param sqlTotal 查询总条数sql
   * @param params 查询的参数
   * @作者 田应平
   * @QQ 444141300
   * @创建时间 2021/6/10 21:07
  */
  public final Future<String> listPage(final String sqlListData,final String sqlTotal,final Map<String,Object> params){
    final Promise<String> promise = Promise.promise();
    final Future<ArrayList<JsonObject>> listData = queryList(sqlListData,params);
    final Future<Integer> total = queryInteger(sqlTotal,params);
    final CompositeFuture all = CompositeFuture.all(total,listData);
    all.onSuccess(handler->{
      final String json = ToolClient.listPage(listData.result(),total.result());
      promise.complete(json);
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }

  //仅供参考
  protected final Future<String> listPageTatol(final String sqlListData,final String sqlTotal,final Map<String,Object> params){
    final Promise<String> promise = Promise.promise();
    final Future<ArrayList<JsonObject>> listData = queryList(sqlListData,params);
    final Future<JsonObject> total = queryMap(sqlTotal,params);
    final CompositeFuture all = CompositeFuture.all(total,listData);
    all.onSuccess(handler->{
      final List<Object> list = handler.list();
      ArrayList<JsonObject> data = null;
      Integer record = null;
      for(int i = 0; i < list.size(); i++){
        final Object o = list.get(i);
        if(o instanceof List){
          data = listData.result();
        }
        if(o instanceof JsonObject){
          final JsonObject jsonObject = (JsonObject)o;
          final Set<String> keys = jsonObject.fieldNames();
          for(final String key : keys){
            record = jsonObject.getInteger(key);
          }
        }
      }
      final String json = ToolClient.listPage(data,record);
      promise.complete(json);
    }).onFailure(err->{
      promise.fail(err.getCause());
    });
    return promise.future();
  }
}