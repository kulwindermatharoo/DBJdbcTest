package org.ksm.integration;

import java.sql.*;
import java.util.Properties;

public class DBUtils {

    public void printResultSet(ResultSet rs, String user, boolean printColumns, boolean printData)
            throws Exception {

        // Get the metadata of the ResultSet
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();

        // Print column names
        if(printColumns) {
            for (int i = 1; i <= columnCount; i++) {
                System.out.print(rsmd.getColumnName(i) + "\t");
            }
            System.out.println();
        }


        // Iterate through the ResultSet and print each row
        int rowCount = 0;
        String name = "";
        while (rs.next()) {
            name = rs.getString(2);
            if(printData) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i) + "\t");
                }
                System.out.println();
            }
            rowCount++;

        }

        if (rowCount != 1) {
            throw new RuntimeException(String.format("%s retuned %d rows ", user, rowCount));
        }

        //user = "jkjkjk";
        if (!name.equalsIgnoreCase(user)) {
            throw new RuntimeException(String.format("[%s] user query result returned [%s] ", user, name));
        }
    }

    public static void runTask(String user) throws Exception{
        String url = DBXJdbcTest.workspace;
        Properties properties = new Properties();
        properties.put("PWD", DBXJdbcTest.token);
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            if (connection != null) {
                Statement statement = connection.createStatement();
                statement.execute("use catalog ksm_test;");
                String useSchemaQuery = String.format("use %s ;", user.toLowerCase());
                statement.execute(useSchemaQuery);

                ResultSet resultSet = statement.executeQuery(" select * from ksm_test.test_schema.test_ksm_view where" +
                        " lower(First_Name) = lower" +
                        "(current_database())");
               new DBUtils().printResultSet(resultSet, user,true,true);
                System.out.println("test end");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void createSchemas(String[] users,String catalogName, boolean drop) {
        String url = DBXJdbcTest.workspace;
        Properties properties = new Properties();
        properties.put("PWD", DBXJdbcTest.token);
        try (Connection connection = DriverManager.getConnection(url, properties)) {
            if (connection != null) {
                Statement statement = connection.createStatement();

                statement.execute("use catalog " + catalogName + ";");
                for (String user : users) {
                    if(drop){
                        statement.execute("drop schema IF EXISTS  " + user);
                    } else {
                        statement.execute("create schema IF NOT EXISTS  " + user);
                    }
                }
                System.out.println("test end");
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

}
