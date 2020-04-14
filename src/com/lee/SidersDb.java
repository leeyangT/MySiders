package com.lee;

import com.mchange.v2.c3p0.C3P0ProxyStatement;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SidersDb {
    private ComboPooledDataSource dataSource;

    public void init(){
        try {
            dataSource = new ComboPooledDataSource();
            dataSource.setJdbcUrl("jdbc:mysql://" + Config.db_host + ":3306/"+ Config.db_name +"?characterEncoding=utf-8");
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
            dataSource.setUser(Config.db_user);
            dataSource.setPassword(Config.db_pwd);
            dataSource.setCheckoutTimeout(20000);       //获取一个connection超时时间，单位毫秒
            dataSource.setInitialPoolSize(10);          //初始化时获取连接数，取值应在minPoolSize与maxPoolSize之间。Default: 3
            dataSource.setAcquireIncrement(20);         //当连接池中的连接耗尽的时候c3p0一次同时获取的连接数。Default: 3
            dataSource.setMinPoolSize(3);               //连接池中保留的最小连接数，默认为：3
            dataSource.setMaxPoolSize(120);             //接池中保留的最大连接数。默认值: 15
            dataSource.setMaxStatements(3000);          //c3p0全局的PreparedStatements缓存的大小。
            //如果maxStatements与maxStatementsPerConnection均为0，则缓存不生效，只要有一个不为0，则语句的缓存就能生效。如果默认值: 0
            dataSource.setMaxIdleTime(3600);            //最大空闲时间，多少秒内未使用则连接被丢弃。若为0则永不丢弃。默认值: 0
            dataSource.setIdleConnectionTestPeriod(10); //每隔多少秒检查所有连接池中的空闲连接。Default: 0
            dataSource.setAcquireRetryAttempts(3000);   //重新尝试的时间间隔，默认为：1000毫秒

        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private Statement getStatement(){
        Connection connection = null;
        Statement statement;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();
            ((C3P0ProxyStatement)statement).rawStatementOperation(com.mysql.jdbc.Statement.class.getMethod("enableStreamingResults"), C3P0ProxyStatement.RAW_STATEMENT, new Object[0]);
        }catch (Exception e){
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (Exception e2){
                    throw new RuntimeException(e2);
                }
            }
            throw new RuntimeException(e);
        }

        return statement;
    }

    public void executeSql(String strSql){
        Statement statement = getStatement();
        try {
            statement.execute(strSql);
        }catch (Exception e){
            throw new RuntimeException(e);
        }
        finally {
            try{
                Connection connection = statement.getConnection();
                connection.close();
                statement.close();
            }catch (Exception e){

            }
        }
    }

    public <T extends Table>List<T> getRowsClass(Class<T> cls, String strSql){
        Statement stmt = getStatement();
        List<T> lstRet = new ArrayList<T>();
        if (stmt  != null) {
            try {
                ResultSet resultSet = stmt .executeQuery(strSql);
                ResultSetMetaData rsmd = resultSet.getMetaData();
                int iColumnCount = rsmd.getColumnCount();
                Field []fields = new Field[iColumnCount];
                for (int i = 1; i <= iColumnCount; i++) {
                    fields[i-1] = cls.getField(rsmd.getColumnName(i));
                }
                while (resultSet.next()) {
                    T table = cls.newInstance();
                    for (int i=1; i < iColumnCount; i++) {
                        fields[i-1].set(table, getResultSetAtom(rsmd, resultSet, i));
                    }
                }
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        }
        return lstRet;
    }

    private static Object getResultSetAtom(ResultSetMetaData rsmd, ResultSet rs, int i) throws Exception {
        int iColumnType = rsmd.getColumnType(i);
        switch (iColumnType) {
            case Types.TINYINT:
                return rs.getByte(i);
            case Types.INTEGER:
                return rs.getInt(i);
            case Types.BIGINT:
                return rs.getLong(i);
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                byte[] arrTmp = rs.getBytes(i);
                //return new String(arrTmp, BaseUtil.s_CharsetUtf8);
            default:
                return rs.getString(i);
        }
    }

}
