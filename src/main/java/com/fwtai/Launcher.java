package com.fwtai;

import com.fwtai.callback.QueryResultMap;
import com.fwtai.dao.DaoHandle;
import com.fwtai.tool.ToolClient;
import io.netty.util.internal.logging.InternalLogger;
import io.netty.util.internal.logging.Log4JLoggerFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.util.ArrayList;
import java.util.HashMap;

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

    //第四步,配置Router解析url
    // http://127.0.0.1:8801/push/v1.0/add?deviceFlag=330602&volume=52.60
    router.get("/push/v1.0/add").handler((context) -> {
      final HashMap<String,String> params = ToolClient.getParams(context);
      ToolClient.validateField(params,"deviceFlag","volume");
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
    final int port = 8801;
    server.requestHandler(router).listen(port,http -> {
      if (http.succeeded()){
        startPromise.complete();
        logger.info("---应用启动成功---,http://127.0.0.1:"+port);
      } else {
        logger.error("Launcher应用启动失败,"+http.cause());
      }
    });
  }
}