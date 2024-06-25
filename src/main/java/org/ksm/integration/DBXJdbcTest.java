package org.ksm.integration;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class DBXJdbcTest {

    public static final String token = "Set you token here";
    public static final String workspace = "jdbc:databricks://set url here";


    static class RunTask implements Callable<String> {

        private final String user;
        RunTask(String user) {
            this.user = user;
        }

        @Override
        public String call() throws Exception {
            String url = workspace;
            Properties properties = new Properties();
            properties.put("PWD", token);
            try (Connection connection = DriverManager.getConnection(url, properties)) {
                if (connection != null) {
                    Statement statement = connection.createStatement();
                    statement.execute("use catalog ksm_test;");
                    String useSchemaQuery = String.format("use %s ;", user.toLowerCase());
                    statement.execute(useSchemaQuery);

                    ResultSet resultSet = statement.executeQuery(" select * from ksm_test.test_schema.test_ksm_view where" +
                            " lower(First_Name) = lower" +
                            "(current_database())");
                    new DBUtils().printResultSet(resultSet, user,false,false);
                    //System.out.println("test end");
                }
            }
            return user;
        }
    }



    public static void main(String[] args) {

        String[] users = {
                //"James",
                "Donald",
                "Douglas",
                "Matthew",
                "Adam",
                "Payam",
                "Shanta",
                "Kevin",
                "Julia",
                "Irene",
                "Steven",
                "Laura",
                "Mozhe",
                "TJ",
                "Jason",
                "Michael",
                "Ki",
                "Hazel",
                "Renske",
                "Stephen",
                "John",
                "Joshua"
        };

        /*
        ArrayList<String> schemas = new ArrayList<>();
        for(int i=0;i<1000;i++){
            schemas.add(String.format("s_%d",i));
        }
        DBUtils.createSchemas(schemas.toArray(new String[0]),"ksm1_test",true);
        System.out.println();
        */

        /*for (String user : users) {
            runTask(user);
        }*/
        //runTask(users[0]);



        // 21 user in one list
        ArrayList<String> userList = new ArrayList<>(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));
        userList.addAll(Arrays.asList(users));

        ////
        ////


        //ksm start

        int numberOfThreads = 200;
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfThreads);

        // List to hold Future objects associated with Callable
        List<Future<String>> futures = new ArrayList<>();

        // Submit tasks to the thread pool
        for (String arg : userList) {
            Future<String> future = executorService.submit(new RunTask(arg));
            futures.add(future);
        }

        // Retrieve results from the futures
        for (Future<String> future : futures) {
            try {
                String result = future.get(); // This will block until the task is complete
                //System.out.println(result);
            } catch (Exception e) {
                System.err.println("Task execution failed: " + e.getMessage());
            }
        }

        // Shut down the executor service
        executorService.shutdown();
        try {
            // Wait for all tasks to complete or timeout after a specified period
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }


    }
}
