/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test;

import com.google.common.collect.Ordering;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

/**
 * A mock database of banking system.
 * 一个模拟的, 基于CSV存储的银行数据库系统
 */
public class BankingDatabase {

    public BankingDatabase() {
        Properties properties = new Properties();
        properties.setProperty(CalciteConnectionProperty.DEFAULT_NULL_COLLATION.camelName(),
                NullCollation.LOW.name());
    }

    private static final Logger logger = LoggerFactory.getLogger(BankingDatabase.class);

    private static String sql0 = "SELECT BRANCH_CITY, assets\n" +
            "FROM ACCOUNT JOIN BRANCH ON ACCOUNT.BRANCH_NAME = BRANCH.BRANCH_NAME\n" +
            "WHERE assets > 1000";

    private static String sql1 = "SELECT BRANCH_CITY, sum(assets) as totalAssets\n" +
            "FROM BRANCH \n" +
            "WHERE BRANCH_CITY in ('Shanghai', 'Hangzhou', 'Beijing', 'Dalian') and assets > 10000 and assets > 1000 \n" +
            "GROUP BY BRANCH_CITY \n" +
            "HAVING sum(assets) > 20000 \n" +
            "ORDER BY BRANCH_CITY desc";

    private static String sql2 = "SELECT BRANCH_CITY, sum(balance) as totalBalance, count(ACCOUNT_NAME) as totalAccount \n" +
            "FROM ACCOUNT \n" +
            "  JOIN BRANCH \n" +
            "    ON ACCOUNT.BRANCH_NAME = BRANCH.BRANCH_NAME \n" +
            "WHERE BRANCH_CITY in ('Shanghai', 'Hangzhou', 'Beijing', 'Dalian') \n" +
            "GROUP BY BRANCH_CITY \n" +
            "HAVING AVG(balance) >= 1000 \n" +
            "ORDER BY BRANCH_CITY desc";

    private static String sql4 = "SELECT BRANCH_CITY\n" +
            "FROM ACCOUNT JOIN BRANCH\n" +
            " ON ACCOUNT.BRANCH_NAME = BRANCH.BRANCH_NAME \n" +
            "WHERE ASSETS >= 1000 ";

    //========================================================================
    //========================== UnitTest Part ===============================
    //========================================================================

    @Test
    public void testParser1() throws Exception {
        SqlParser sqlParser = SqlParser.create(sql1);
        SqlNode sqlNode = sqlParser.parseQuery();
        sqlNode.accept(new DumpSqlVisitor());
    }

    @Test
    public void testParser2() throws Exception {
        SqlParser sqlParser = SqlParser.create(sql2);
        SqlNode sqlNode = sqlParser.parseQuery();
        sqlNode.accept(new DumpSqlVisitor());
    }

    @Test
    public void testExecuter1() throws Exception {
        BankingDatabase database = new BankingDatabase();
        database.singleQuery(sql0, "banking");
    }

    @Test
    public void testExecuter2() throws Exception {
        BankingDatabase database = new BankingDatabase();
        database.singleQuery(sql2, "banking");
    }

    @Test
    public void testExecuter3() throws Exception {
        BankingDatabase database = new BankingDatabase();
        database.singleQuery(sql4, "banking");
    }

    /*
     * 请检查 org.apache.calcite.test.SqlToRelConverterTest
     */

    /**
     * 通过avatica查询
     *
     * @param sql   sql
     * @param model schema
     */
    private void singleQuery(String sql, String model)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            logger.info("连接数据库...");
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            output(resultSet);
            resultSet.close();
        } finally {
            close(connection, statement);
        }
    }

    //========================================================================
    //========================== Utility  Part ===============================
    //========================================================================

    private Fluent sql(String model, String sql) {
        return new Fluent(model, sql, this::output);
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private static Consumer<ResultSet> expect(final String... expected) {
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                BankingDatabase.collect(lines, resultSet);
                Assert.assertEquals(Arrays.asList(expected), lines);
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        };
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private static Consumer<ResultSet> expectUnordered(String... expected) {
        final List<String> expectedLines =
                Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
        return resultSet -> {
            try {
                final List<String> lines = new ArrayList<>();
                BankingDatabase.collect(lines, resultSet);
                Collections.sort(lines);
                Assert.assertEquals(expectedLines, lines);
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        };
    }

    private void checkSql(String sql, String model, Consumer<ResultSet> fn)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            logger.info("连接数据库...");
            statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(sql);
            fn.accept(resultSet);
        } finally {
            close(connection, statement);
        }
    }

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        return Sources.of(BankingDatabase.class.getResource("/" + path)).file().getAbsolutePath();
    }

    private static void collect(List<String> result, ResultSet resultSet)
            throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            buf.setLength(0);
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for (int i = 1; i <= n; i++) {
                buf.append(sep)
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getString(i));
                sep = "; ";
            }
            result.add(Util.toLinux(buf.toString()));
        }
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        logger.info("输出结果:");
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    /**
     * Creates a command that appends a line to the CSV file.
     */
    private Callable<Void> writeLine(final PrintWriter pw, final String line) {
        return () -> {
            pw.println(line);
            pw.flush();
            return null;
        };
    }

    /**
     * Creates a command that sleeps.
     */
    private Callable<Void> sleep(final long millis) {
        return () -> {
            Thread.sleep(millis);
            return null;
        };
    }

    /**
     * Creates a command that cancels a statement.
     */
    private Callable<Void> cancel(final Statement statement) {
        return () -> {
            statement.cancel();
            return null;
        };
    }

    private Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            throw TestUtil.rethrow(e);
        }
        return null;
    }

    /**
     * Receives commands on a queue and executes them on its own thread.
     * Call {@link #close} to terminate.
     *
     * @param <E> Result value of commands
     */
    private static class Worker<E> implements Runnable, AutoCloseable {
        /**
         * Queue of commands.
         */
        final BlockingQueue<Callable<E>> queue =
                new ArrayBlockingQueue<>(5);

        /**
         * Value returned by the most recent command.
         */
        private E v;

        /**
         * Exception thrown by a command or queue wait.
         */
        private Exception e;

        /**
         * The poison pill command.
         */
        final Callable<E> end = () -> null;

        public void run() {
            try {
                for (; ; ) {
                    final Callable<E> c = queue.take();
                    if (c == end) {
                        return;
                    }
                    this.v = c.call();
                }
            } catch (Exception e) {
                this.e = e;
            }
        }

        public void close() {
            try {
                queue.put(end);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    /**
     * Fluent API to perform test actions.
     */
    private class Fluent {
        private final String model;
        private final String sql;
        private final Consumer<ResultSet> expect;

        Fluent(String model, String sql, Consumer<ResultSet> expect) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }

        /**
         * Runs the test.
         */
        Fluent ok() {
            try {
                checkSql(sql, model, expect);
                return this;
            } catch (SQLException e) {
                throw TestUtil.rethrow(e);
            }
        }

        /**
         * Assigns a function to call to test whether output is correct.
         */
        Fluent checking(Consumer<ResultSet> expect) {
            return new Fluent(model, sql, expect);
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query.
         */
        Fluent returns(String... expectedLines) {
            return checking(expect(expectedLines));
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query,
         * in no particular order.
         */
        Fluent returnsUnordered(String... expectedLines) {
            return checking(expectUnordered(expectedLines));
        }
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }


    /**
     * Quotes a string for Java or JSON.
     */
    private static String escapeString(String s) {
        return escapeString(new StringBuilder(), s).toString();
    }

    /**
     * Quotes a string for Java or JSON, into a builder.
     */
    private static StringBuilder escapeString(StringBuilder buf, String s) {
        buf.append('"');
        int n = s.length();
        char lastChar = 0;
        for (int i = 0; i < n; ++i) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    buf.append("\\\\");
                    break;
                case '"':
                    buf.append("\\\"");
                    break;
                case '\n':
                    buf.append("\\n");
                    break;
                case '\r':
                    if (lastChar != '\n') {
                        buf.append("\\r");
                    }
                    break;
                default:
                    buf.append(c);
                    break;
            }
            lastChar = c;
        }
        return buf.append('"');
    }
}

// End CsvTest.java
