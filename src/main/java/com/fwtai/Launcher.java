package com.fwtai;

import com.fwtai.callback.QueryResultMap;
import com.fwtai.dao.DaoHandle;
import com.fwtai.tool.ToolClient;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 异步处理方式;区别在于 router.get("/sync").handler 和 router.get("/async").blockingHandler,即异步是 handler;同步阻塞是 blockingHandler
 * @作者 田应平
 * @版本 v1.0
 * @创建时间 2020年9月17日 13:27:41
 * @QQ号码 444141300
 * @Email service@dwlai.com
 * @官网 http://www.fwtai.com
*/
public class Launcher extends AbstractVerticle {

  final InternalLogger logger = Log4JLoggerFactory.getInstance(getClass());

  private Router router;

  @Override
  public void start(final Promise<Void> startPromise) throws Exception {

    final DaoHandle daoHandle = new DaoHandle(vertx);

    final HttpServer server = vertx.createHttpServer();

    router = Router.router(vertx);

    router.route().handler(BodyHandler.create());

    router.get("/").handler(context->{
      ToolClient.responseJson(context,ToolClient.createJson(200,"欢迎访问本服务平台"));
    });

    // http://127.0.0.1:808/queryList
    router.get("/queryList").handler(context->{
      final String sqlListData = "SELECT kid,username,`password` from sys_user ORDER BY username DESC LIMIT 0,5";
      final List<Object> paramsListData = new ArrayList<>(0);
      daoHandle.queryList(context,sqlListData,paramsListData);
    });

    // http://127.0.0.1:808/queryList1?username=tz&section=1
    router.get("/queryList1").handler(context->{
      final String username = context.request().getParam("username");
      final String section = context.request().getParam("section");
      final StringBuilder sql = new StringBuilder("SELECT kid,username,`password` from sys_user ");
      final ArrayList<Object> objects = new ArrayList<>(2);
      if(username != null && username.length() > 0){
        sql.append("where username LIKE CONCAT('%',?,'%') ");
        objects.add(username);
      }
      sql.append("ORDER BY username DESC ");
      if(section != null && section.length() > 0){
        sql.append("LIMIT ?,5");
        objects.add(Integer.parseInt(section));
      }
      System.out.println(sql.toString());
      daoHandle.queryList(sql.toString(),objects).onSuccess(list->{
        ToolClient.responseJson(context,ToolClient.queryJson(list));
      }).onFailure(err->{
        logger.error("queryList1异常,"+err.getMessage());
        ToolClient.responseJson(context,ToolClient.createJson(199,"连接数据库失败"));
      });
    });

    // http://127.0.0.1:808/queryMap1
    router.get("/queryMap1").handler(context->{
      //final String sql = "SELECT kid,username,`password` from sys_user ORDER BY username DESC LIMIT 1";
      final String sql = "SELECT COUNT(kid) total from sys_user LIMIT 1";
      daoHandle.queryMap(sql,new ArrayList<Object>(0)).onSuccess(map->{
        ToolClient.responseJson(context,ToolClient.queryJson(map));
      }).onFailure(err->{
        logger.error("queryMap1异常,"+err.getMessage());
        ToolClient.responseJson(context,ToolClient.createJson(199,"连接数据库失败"));
      });
    });

    // http://127.0.0.1:808/queryMap2
    router.get("/queryMap2").handler(context->{
      //final String sql = "SELECT kid,username,`password` from sys_user ORDER BY username DESC LIMIT 1";
      final String sql = "SELECT COUNT(kid) total from sys_user LIMIT 1";
      daoHandle.queryMap(context,sql,new ArrayList<>(0));
    });

    // http://127.0.0.1:808/execute101?username=ad&password=21012
    router.get("/execute101").handler(context->{
      final String sql = "UPDATE sys_user SET `password` = #{password} WHERE username = #{username} LIMIT 1";
      final String username = context.request().getParam("username");
      final String password = context.request().getParam("password");
      final Map<String,Object> parameters = new HashMap<>();
      parameters.put("username",username);
      parameters.put("password",password);
      daoHandle.execute(sql,parameters).onSuccess(rows->{
        //ToolClient.responseJson(context,ToolClient.executeRows(rows,"操作成功","操作失败"));//ok
        ToolClient.responseJson(context,ToolClient.executeRows(rows));
      }).onFailure(err->{
        ToolClient.responseJson(context,ToolClient.createJson(199,"系统出现错误,"+err));
      });
    });

    // http://127.0.0.1:808/queryMap11?username=admin
    router.get("/queryMap11").handler(context->{
      //final String sql = "SELECT COUNT(kid) total from sys_user where username LIKE CONCAT('%',?,'%') LIMIT 1";
      final String sql = "SELECT kid,username,`password` from sys_user where username LIKE CONCAT('%',#{username},'%') LIMIT 1";
      final String username = context.request().getParam("username");
      final Map<String,Object> parameters = new HashMap<>();
      parameters.put("username",username);
      daoHandle.queryMap(sql,parameters).onSuccess(map->{
        ToolClient.responseJson(context,ToolClient.queryJson(map));
      }).onFailure(err->{
        ToolClient.responseJson(context,ToolClient.createJson(199,"系统出现错误,"+err));
      });
    });

    // http://127.0.0.1:808/queryList1021?username=t
    router.get("/queryList1021").handler(context->{
      //final String sql = "SELECT COUNT(kid) total from sys_user where username LIKE CONCAT('%',?,'%') LIMIT 1";
      final String sql = "SELECT kid,username,`password` from sys_user where username LIKE CONCAT('%',#{username},'%')";
      final String username = context.request().getParam("username");
      final Map<String,Object> parameters = new HashMap<>();
      parameters.put("username",username);
      daoHandle.queryList(sql,parameters).onSuccess(list->{
        ToolClient.responseJson(context,ToolClient.queryJson(list));
      }).onFailure(err->{
        ToolClient.responseJson(context,ToolClient.createJson(199,"系统出现错误,"+err));
      });
    });

    // todo 分页 http://127.0.0.1:808/listDataTotal
    router.get("/listDataTotal").handler(context->{
      final String sqlListData = "SELECT kid,username,`password` from sys_user ORDER BY username DESC LIMIT 0,5";
      final List<Object> paramsListData = new ArrayList<>(0);
      final String sqlTotal = "SELECT COUNT(kid) total from sys_user LIMIT 1";
      final List<Object> paramsTotal = new ArrayList<>(0);
      final Future<ArrayList<JsonObject>> listData = daoHandle.queryList(sqlListData,paramsListData);
      final Future<JsonObject> total = daoHandle.queryMap(sqlTotal,paramsTotal);
      final CompositeFuture all = CompositeFuture.all(total,listData);
      all.onSuccess(handler->{
        System.out.println("两个都操作成功");
        System.out.println(handler.list());//包含全部数据
        final int size = handler.size();
        final List<Object> list = handler.list();
        List<JsonObject> data = null;
        Integer record = 0;
        for(int i = 0; i < list.size(); i++){
          final Object o = list.get(i);
          if(o instanceof List){
            System.out.println(o);
            data = (List<JsonObject>) o;
          }
          if(o instanceof JsonObject){
            System.out.println(o);
            record = ((JsonObject)o).getInteger("total");
          }
        }
        final String json = ToolClient.listPage(data,record);
        ToolClient.responseJson(context,json);
      }).onFailure(err->{
        if(listData.failed()){
          listData.cause().printStackTrace();
        }
        if(total.failed()){
          total.cause().printStackTrace();
        }
        ToolClient.responseJson(context,ToolClient.createJson(199,"系统出现错误"));
      });
    });

    // todo 分页 http://127.0.0.1:808/listPage?username=ad
    router.get("/listPage").handler(context->{
      final String username = context.request().getParam("username");
      final String sqlListData = "SELECT kid,username,`password` from sys_user where username LIKE CONCAT('%',#{username},'%') ORDER BY username DESC LIMIT 0,5";
      final String sqlTotal = "SELECT COUNT(kid) total from sys_user where username LIKE CONCAT('%',#{username},'%') LIMIT 1";
      final HashMap<String,Object> params = new HashMap<>();
      ToolClient.composeParams(params,"username",username);
      daoHandle.listPage(sqlListData,sqlTotal,params).onSuccess(json->{
        ToolClient.responseJson(context,json);
      }).onFailure(throwable -> {
        throwable.printStackTrace();
        ToolClient.responseJson(context,ToolClient.createJson(199,"系统出现错误,"+throwable.getMessage()));
      });
    });

    //第四步,配置Router解析url
    // http://127.0.0.1:808/push/v1.0/add?deviceFlag=330602&volume=52.60
    router.get("/push/v1.0/add").handler((context) -> {
      final HashMap<String,String> params = ToolClient.getParams(context);
      final String validateField = ToolClient.validateField(params,"deviceFlag","volume");
      if(validateField != null){
        ToolClient.responseJson(context,validateField);
        return;
      }
      final HttpServerRequest request = context.request();
      final String deviceFlag = request.getParam("deviceFlag");
      final String volume = request.getParam("volume");
      final ArrayList<Object> sqlParams = new ArrayList<>();
      final String sql = "SELECT kid,volume FROM monitor_value WHERE device_flag = ? LIMIT 1";
      sqlParams.add(deviceFlag);
      daoHandle.queryMap(sql,sqlParams,new QueryResultMap(){
        @Override
        public void succeed(final JsonObject jsonObject){
          System.out.println("jsonObject->"+jsonObject);
          final String locationId = jsonObject.getString("kid");
          if(locationId == null){
            ToolClient.responseJson(context,ToolClient.createJson(199,deviceFlag+"设备标识不存在,拒绝接入"));
          }else{
            final String sqlAdd = "INSERT INTO MONITOR_VALUE (device_flag,volume)" + " VALUES (?,?)";
            final ArrayList<Object> paramsAdd = new ArrayList<>();
            paramsAdd.add(deviceFlag);
            paramsAdd.add(volume);
            daoHandle.execute(context,sqlAdd,paramsAdd);
          }
        }
        @Override
        public void failure(final Throwable throwable){
          ToolClient.responseJson(context,ToolClient.createJson(199,"连接数据库失败"));
        }
      });
    });

    //全局异常处理,放在最后一个route
    router.route().last().handler(context -> {
      ToolClient.responseJson(context,ToolClient.createJson(404,"访问的url不存在"));
    }).failureHandler(context -> {
      ToolClient.responseJson(context,ToolClient.createJson(204,"操作失败,系统出现错误"));
    });
    final int port = 808;
    server.requestHandler(router).listen(port,http -> {
      if (http.succeeded()){
        startPromise.complete();//todo 告知vert启动完毕
        logger.info("---应用启动成功---,http://127.0.0.1:"+port);
      } else {
        logger.error("Launcher应用启动失败,"+http.cause());
      }
    });
  }

  protected void redirect(final RoutingContext context){
    context.response().setStatusCode(302).putHeader("Location","http://www.dwz.cloud").end();
  }
}