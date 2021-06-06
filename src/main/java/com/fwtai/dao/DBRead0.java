package com.fwtai.dao;

import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.PoolOptions;

/**
 * 读库
 * @作者 田应平
 * @版本 v1.0
 * @创建时间 2021-02-04 9:45
 * @QQ号码 444141300
 * @Email service@dwlai.com
 * @官网 http://www.fwtai.com
*/
public final class DBRead0{

  // 创建数据库连接池
  private MySQLPool client;

  private MySQLConnectOptions connectOptions;

  public DBRead0(final Vertx vertx){
    connectOptions = new MySQLConnectOptions()
      .setPort(3308)
      .setHost("192.168.3.66")
      .setDatabase("vertx")
      .setUser("root")
      .setPassword("rootFwtai")
      .setCharset("utf8mb4")
      .setSsl(false);
    //配置数据库连接池
    final PoolOptions pool = new PoolOptions().setMaxSize(8);
    client = MySQLPool.pool(vertx,connectOptions,pool);
  }

  protected MySQLPool getClient(){
    return client;
  }
}