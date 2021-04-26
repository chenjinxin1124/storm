//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package cjx.com.trident.kfkTridentJDBC;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.*;
import java.util.*;

public class KfkJdbcClient {
    private static final Logger LOG = LoggerFactory.getLogger(KfkJdbcClient.class);
    private ConnectionProvider connectionProvider;
    private int queryTimeoutSecs;

    public KfkJdbcClient(ConnectionProvider connectionProvider, int queryTimeoutSecs) {
        this.connectionProvider = connectionProvider;
        this.queryTimeoutSecs = queryTimeoutSecs;
    }

    public void insert(String tableName, List<List<Column>> columnLists) {
        String query = this.constructInsertQuery(tableName, columnLists);
        this.executeInsertQuery(query, columnLists);
    }



    public void executeSelectAndInsertOrUpdate(String query, List<List<Column>> columnLists) {
        Connection connection = null;

        try {
            connection = this.connectionProvider.getConnection();
            boolean autoCommit = connection.getAutoCommit();
            if(autoCommit) {
                connection.setAutoCommit(false);
            }

            LOG.debug("Executing query {}", query);
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            if(this.queryTimeoutSecs > 0) {
                preparedStatement.setQueryTimeout(this.queryTimeoutSecs);
            }

            Iterator var6 = columnLists.iterator();

            while(var6.hasNext()) {
                List<Column> columnList = (List)var6.next();
                this.setPreparedStatementParams(preparedStatement, columnList);
                preparedStatement.addBatch();
            }

            int[] results = preparedStatement.executeBatch();
            if(Arrays.asList(new int[][]{results}).contains(Integer.valueOf(-3))) {
                connection.rollback();
                throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
            }

            try {
                connection.commit();
            } catch (SQLException var12) {
                throw new RuntimeException("Failed to commit insert query " + query, var12);
            }
        } catch (SQLException var13) {
            throw new RuntimeException("Failed to execute insert query " + query, var13);
        } finally {
            this.closeConnection(connection);
        }

    }


    public void executeInsertQuery(String query, List<List<Column>> columnLists) {
        Connection connection = null;

        try {
            connection = this.connectionProvider.getConnection();
            boolean autoCommit = connection.getAutoCommit();
            if(autoCommit) {
                connection.setAutoCommit(false);
            }

            LOG.debug("Executing query {}", query);
            PreparedStatement preparedStatement = connection.prepareStatement(query);
            if(this.queryTimeoutSecs > 0) {
                preparedStatement.setQueryTimeout(this.queryTimeoutSecs);
            }

            Iterator var6 = columnLists.iterator();

            while(var6.hasNext()) {
                List<Column> columnList = (List)var6.next();
                this.setPreparedStatementParams(preparedStatement, columnList);
                preparedStatement.addBatch();
            }

            int[] results = preparedStatement.executeBatch();
            if(Arrays.asList(new int[][]{results}).contains(Integer.valueOf(-3))) {
                connection.rollback();
                throw new RuntimeException("failed at least one sql statement in the batch, operation rolled back.");
            }

            try {
                connection.commit();
            } catch (SQLException var12) {
                throw new RuntimeException("Failed to commit insert query " + query, var12);
            }
        } catch (SQLException var13) {
            throw new RuntimeException("Failed to execute insert query " + query, var13);
        } finally {
            this.closeConnection(connection);
        }

    }

    private String constructInsertQuery(String tableName, List<List<Column>> columnLists) {
        StringBuilder sb = new StringBuilder();
        sb.append("Insert into ").append(tableName).append(" (");
        Collection<String> columnNames = Collections2.transform((Collection)columnLists.get(0), new Function<Column, String>() {
            public String apply(Column input) {
                return input.getColumnName();
            }
        });
        String columns = Joiner.on(",").join(columnNames);
        sb.append(columns).append(") values ( ");
        String placeHolders = StringUtils.chop(StringUtils.repeat("?,", columnNames.size()));
        sb.append(placeHolders).append(")");
        return sb.toString();
    }


    /**
     * 主要是获取排序之后的数据
     * @param sqlQuery
     * @return
     */
    public List<List<Column>> selectAll(String sqlQuery) {
        Connection connection = null;

        ArrayList var19;
        try {
            connection = this.connectionProvider.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
            if(this.queryTimeoutSecs > 0) {
                preparedStatement.setQueryTimeout(this.queryTimeoutSecs);
            }

            ResultSet resultSet = preparedStatement.executeQuery();
            ArrayList rows = Lists.newArrayList();

            while(resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<Column> row = Lists.newArrayList();

                for(int i = 1; i <= columnCount; ++i) {
                    String columnLabel = metaData.getColumnLabel(i);
                    int columnType = metaData.getColumnType(i);
                    Class columnJavaType = Util.getJavaType(columnType);
                    if(columnJavaType.equals(String.class)) {
                        row.add(new Column(columnLabel, resultSet.getString(columnLabel), columnType));
                    } else if(columnJavaType.equals(Integer.class)) {
                        row.add(new Column(columnLabel, Integer.valueOf(resultSet.getInt(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Double.class)) {
                        row.add(new Column(columnLabel, Double.valueOf(resultSet.getDouble(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Float.class)) {
                        row.add(new Column(columnLabel, Float.valueOf(resultSet.getFloat(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Short.class)) {
                        row.add(new Column(columnLabel, Short.valueOf(resultSet.getShort(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Boolean.class)) {
                        row.add(new Column(columnLabel, Boolean.valueOf(resultSet.getBoolean(columnLabel)), columnType));
                    } else if(columnJavaType.equals(byte[].class)) {
                        row.add(new Column(columnLabel, resultSet.getBytes(columnLabel), columnType));
                    } else if(columnJavaType.equals(Long.class)) {
                        row.add(new Column(columnLabel, Long.valueOf(resultSet.getLong(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Date.class)) {
                        row.add(new Column(columnLabel, resultSet.getDate(columnLabel), columnType));
                    } else if(columnJavaType.equals(Time.class)) {
                        row.add(new Column(columnLabel, resultSet.getTime(columnLabel), columnType));
                    } else {
                        if(!columnJavaType.equals(Timestamp.class)) {
                            throw new RuntimeException("type =  " + columnType + " for column " + columnLabel + " not supported.");
                        }

                        row.add(new Column(columnLabel, resultSet.getTimestamp(columnLabel), columnType));
                    }
                }

                rows.add(row);
            }

            var19 = rows;
        } catch (SQLException var17) {
            throw new RuntimeException("Failed to execute select query " + sqlQuery, var17);
        } finally {
            this.closeConnection(connection);
        }

        return var19;
    }


    public List<List<Column>> select(String sqlQuery, List<Column> queryParams) {
        Connection connection = null;

        ArrayList var19;
        try {
            connection = this.connectionProvider.getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement(sqlQuery);
            if(this.queryTimeoutSecs > 0) {
                preparedStatement.setQueryTimeout(this.queryTimeoutSecs);
            }

            this.setPreparedStatementParams(preparedStatement, queryParams);
            ResultSet resultSet = preparedStatement.executeQuery();
            ArrayList rows = Lists.newArrayList();

            while(resultSet.next()) {
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();
                List<Column> row = Lists.newArrayList();

                for(int i = 1; i <= columnCount; ++i) {
                    String columnLabel = metaData.getColumnLabel(i);
                    int columnType = metaData.getColumnType(i);
                    Class columnJavaType = Util.getJavaType(columnType);
                    if(columnJavaType.equals(String.class)) {
                        row.add(new Column(columnLabel, resultSet.getString(columnLabel), columnType));
                    } else if(columnJavaType.equals(Integer.class)) {
                        row.add(new Column(columnLabel, Integer.valueOf(resultSet.getInt(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Double.class)) {
                        row.add(new Column(columnLabel, Double.valueOf(resultSet.getDouble(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Float.class)) {
                        row.add(new Column(columnLabel, Float.valueOf(resultSet.getFloat(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Short.class)) {
                        row.add(new Column(columnLabel, Short.valueOf(resultSet.getShort(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Boolean.class)) {
                        row.add(new Column(columnLabel, Boolean.valueOf(resultSet.getBoolean(columnLabel)), columnType));
                    } else if(columnJavaType.equals(byte[].class)) {
                        row.add(new Column(columnLabel, resultSet.getBytes(columnLabel), columnType));
                    } else if(columnJavaType.equals(Long.class)) {
                        row.add(new Column(columnLabel, Long.valueOf(resultSet.getLong(columnLabel)), columnType));
                    } else if(columnJavaType.equals(Date.class)) {
                        row.add(new Column(columnLabel, resultSet.getDate(columnLabel), columnType));
                    } else if(columnJavaType.equals(Time.class)) {
                        row.add(new Column(columnLabel, resultSet.getTime(columnLabel), columnType));
                    } else {
                        if(!columnJavaType.equals(Timestamp.class)) {
                            throw new RuntimeException("type =  " + columnType + " for column " + columnLabel + " not supported.");
                        }

                        row.add(new Column(columnLabel, resultSet.getTimestamp(columnLabel), columnType));
                    }
                }

                rows.add(row);
            }

            var19 = rows;
        } catch (SQLException var17) {
            throw new RuntimeException("Failed to execute select query " + sqlQuery, var17);
        } finally {
            this.closeConnection(connection);
        }

        return var19;
    }

    public List<Column> getColumnSchema(String tableName) {
        Connection connection = null;
        ArrayList columns = new ArrayList();

        try {
            connection = this.connectionProvider.getConnection();
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet resultSet = metaData.getColumns((String)null, (String)null, tableName, (String)null);

            while(resultSet.next()) {
                columns.add(new Column(resultSet.getString("COLUMN_NAME"), resultSet.getInt("DATA_TYPE")));
            }

            ArrayList var6 = columns;
            return var6;
        } catch (SQLException var10) {
            throw new RuntimeException("Failed to get schema for table " + tableName, var10);
        } finally {
            this.closeConnection(connection);
        }
    }

    public void executeSql(String sql) {
        Connection connection = null;

        try {
            connection = this.connectionProvider.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(sql);
        } catch (SQLException var7) {
            throw new RuntimeException("Failed to execute SQL", var7);
        } finally {
            this.closeConnection(connection);
        }

    }

    private void setPreparedStatementParams(PreparedStatement preparedStatement, List<Column> columnList) throws SQLException {
        int index = 1;

        for(Iterator var4 = columnList.iterator(); var4.hasNext(); ++index) {
            Column column = (Column)var4.next();
            Class columnJavaType = Util.getJavaType(column.getSqlType());
            if(column.getVal() == null) {
                preparedStatement.setNull(index, column.getSqlType());
            } else if(columnJavaType.equals(String.class)) {
                preparedStatement.setString(index, (String)column.getVal());
            } else if(columnJavaType.equals(Integer.class)) {
                preparedStatement.setInt(index, ((Integer)column.getVal()).intValue());
            } else if(columnJavaType.equals(Double.class)) {
                preparedStatement.setDouble(index, ((Double)column.getVal()).doubleValue());
            } else if(columnJavaType.equals(Float.class)) {
                preparedStatement.setFloat(index, ((Float)column.getVal()).floatValue());
            } else if(columnJavaType.equals(Short.class)) {
                preparedStatement.setShort(index, ((Short)column.getVal()).shortValue());
            } else if(columnJavaType.equals(Boolean.class)) {
                preparedStatement.setBoolean(index, ((Boolean)column.getVal()).booleanValue());
            } else if(columnJavaType.equals(byte[].class)) {
                preparedStatement.setBytes(index, (byte[])((byte[])column.getVal()));
            } else if(columnJavaType.equals(Long.class)) {
                preparedStatement.setLong(index, ((Long)column.getVal()).longValue());
            } else if(columnJavaType.equals(Date.class)) {
                preparedStatement.setDate(index, (Date)column.getVal());
            } else if(columnJavaType.equals(Time.class)) {
                preparedStatement.setTime(index, (Time)column.getVal());
            } else {
                if(!columnJavaType.equals(Timestamp.class)) {
                    throw new RuntimeException("Unknown type of value " + column.getVal() + " for column " + column.getColumnName());
                }

                preparedStatement.setTimestamp(index, (Timestamp)column.getVal());
            }
        }

    }

    private void closeConnection(Connection connection) {
        if(connection != null) {
            try {
                connection.close();
            } catch (SQLException var3) {
                throw new RuntimeException("Failed to close connection", var3);
            }
        }

    }
}
