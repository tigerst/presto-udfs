package com.presto.tools;

import java.sql.*;

public class PrestoQueryMonitor {
    private static final String PRESTO_DRIVER_NAME = "com.facebook.presto.jdbc.PrestoDriver";
    private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";
    private static final String MYSQL_STT_USER_DB_URL = "jdbc:mysql://10.0.250.189:3306/capacity";
    private static final String MYSQL_STT_USER_USER = "cap_user";
    private static final String MYSQL_STT_USER_PASS = "BIcap!@#";

    private static Statement getPrestoStmt(String host){
        Connection conn = null;
        Statement stmtpresto=null;
        String url="jdbc:presto://"+host;

        try {
            Class.forName(PRESTO_DRIVER_NAME);
            Connection connection=  DriverManager.getConnection(url, "master", null);
            connection.setCatalog("mysql3");
            connection.setSchema("capacity");
            stmtpresto =connection.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return stmtpresto;
    }

    private static Connection getMysqlConn() {
        Connection conn = null;
        try {
            Class.forName(MYSQL_DRIVER_NAME);
            conn = DriverManager.getConnection(MYSQL_STT_USER_DB_URL, MYSQL_STT_USER_USER,
                    MYSQL_STT_USER_PASS);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void getQueryDetail(String host){
        Statement  stmtpresto= getPrestoStmt(host);

        try {
            Connection mysqlcon=getMysqlConn();
            PreparedStatement mysqlStmt =mysqlcon.prepareStatement( "INSERT INTO  capacity.presto_query_detail (query_id," +
                    "node_id,state,user,source,started,end_time,elapsedTime,query_sql,queued_time_ms,analysis_time_ms," +
                    "distributed_planning_time_ms) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");

            int i=0;
            ResultSet rs=stmtpresto.executeQuery("select query_id,node_id,state,user,source,started,last_heartbeat , " +
                    "(to_unixtime(last_heartbeat)-to_unixtime(created)) as elapsedTime ,query ,queued_time_ms,analysis_time_ms," +
                    "distributed_planning_time_ms FROM system.runtime.queries where state in('FINISHED','FAILED') and " +
                    "query not like '%query_id,node_id%' and query not like '%query_id,node_id%'  and " +
                    "date_format(last_heartbeat,'%Y-%m-%d')= get_date(0,'%Y-%m-%d') and query_id not in (select query_id " +
                    " FROM mysql3.capacity.presto_query_detail where date_format(end_time,'%Y-%m-%d')= get_date(0,'%Y-%m-%d') )");
            while (rs.next()){
                mysqlStmt.setString(1,rs.getString(1));
                mysqlStmt.setString(2,rs.getString(2));
                mysqlStmt.setString(3,rs.getString(3));
                mysqlStmt.setString(4,rs.getString(4));
                mysqlStmt.setString(5,rs.getString(5));
                mysqlStmt.setTimestamp(6,rs.getTimestamp(6));
                mysqlStmt.setTimestamp(7,rs.getTimestamp(7));
                double elapsedTime= Double.parseDouble(rs.getString("elapsedTime"));
                mysqlStmt.setDouble(8, elapsedTime);
                mysqlStmt.setString(9,rs.getString(9));
                mysqlStmt.setLong(10, rs.getLong(10));
                mysqlStmt.setLong(11,rs.getLong(11));
                mysqlStmt.setLong(12,rs.getLong(12));
                mysqlStmt.executeUpdate();
                i++;
            }
            System.out.println("getQueryDetail record count: "+i);
            mysqlStmt.close();
            mysqlcon.close();
            stmtpresto.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }

    public static void getTasksDetail(String host){
        Statement   stmtpresto2= getPrestoStmt(host);
        try {
            Connection mysqlcon2=getMysqlConn();
            PreparedStatement mysqlStmt2 =mysqlcon2.prepareStatement( "INSERT INTO  capacity.presto_tasks_detail (query_id," +
                    "node_id,stage_id,state,splits,split_blocked_time_ms,processed_input_bytes,processed_input_rows," +
                    "created,start,end_time,used_time) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)");


            ResultSet  rs2=stmtpresto2.executeQuery("select query_id,node_id ,stage_id,state,splits,split_blocked_time_ms," +
                    "processed_input_bytes,processed_input_rows,created,start,last_heartbeat,(unixtime(last_heartbeat)-unixtime(created)) " +
                    "as used_time from system.runtime.tasks where  query_id in (SELECT  query_id FROM system.runtime.queries where state " +
                    "in('FINISHED','FAILED') and query not like '%query_id,node_id%' and  date_format(last_heartbeat,'%Y-%m-%d')=" +
                    "get_date(0,'%Y-%m-%d') and query_id not in (SELECT  query_id  FROM mysql3.capacity.presto_query_detail " +
                    "where date_format(end_time,'%Y-%m-%d')= get_date(0,'%Y-%m-%d') ) )");
            int i=0;
            while (rs2.next()){
                mysqlStmt2.setString(1,rs2.getString(1));
                mysqlStmt2.setString(2,rs2.getString(2));
                mysqlStmt2.setString(3,rs2.getString(3));
                mysqlStmt2.setString(4,rs2.getString(4));
                mysqlStmt2.setLong(5, rs2.getLong(5));
                mysqlStmt2.setLong(6, rs2.getLong(6));
                mysqlStmt2.setLong(7, rs2.getLong(7));
                mysqlStmt2.setLong(8, rs2.getLong(8));
                mysqlStmt2.setTimestamp(9,rs2.getTimestamp(9));
                mysqlStmt2.setTimestamp(10,rs2.getTimestamp(10));
                mysqlStmt2.setTimestamp(11,rs2.getTimestamp(11));
                double elapsedTime= rs2.getLong(12);
                mysqlStmt2.setDouble(12, elapsedTime);
                mysqlStmt2.executeUpdate();
                i++;
            }

            System.out.println("getTasksDetail record count: "+i);
            mysqlStmt2.close();
            mysqlcon2.close();
            stmtpresto2.close();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


//    public static void  main(String[] args) throws SQLException {
//        getQueryDetail(args[0]);
//        getTasksDetail(args[0]);
//
//    }
}
