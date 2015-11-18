package com.chariotsolutions.jshepard.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;

import static org.junit.Assert.*;

public abstract class BaseJdbcTest {
  LocalDate date = LocalDate.of(2000, Month.JANUARY, 05);
  LocalTime time = LocalTime.of(12, 34, 56, 789054321);

  ResultSetWrapper getTimestampResultSetWrapper(Connection connection, LocalDateTime now) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    connection.createStatement().execute("create table test1 (id int primary key, test1 timestamp, test2 date, test3 time)");
    PreparedStatement statement = connection.prepareStatement("insert into test1 values (?, ?, ?, ?)");
    statement.setInt(1, 1);
    PreparedStatementWrapper statementWrapper = new PreparedStatementWrapper(statement);
    statementWrapper.setLocalDateTime(2, now);
    statementWrapper.setLocalDate(3, date);
    statementWrapper.setLocalTime(4, time);
    assertEquals(1, statement.executeUpdate());
    ResultSet resultSet = connection.createStatement().executeQuery("select * from test1");
    assertTrue(resultSet.next());
    return new ResultSetWrapper(metaData, resultSet);
  }

  ResultSetWrapper getTimestampResultSetWrapperWithNulls(Connection connection) throws SQLException {
    DatabaseMetaData metaData = connection.getMetaData();
    connection.createStatement().execute("create table test1 (id int primary key, test1 timestamp, test2 date, test3 time)");
    PreparedStatement statement = connection.prepareStatement("insert into test1 values (?, ?, ?, ?)");
    statement.setInt(1, 1);
    PreparedStatementWrapper statementWrapper = new PreparedStatementWrapper(statement);
    statementWrapper.setLocalDateTime(2, null);
    statementWrapper.setLocalDate(3, null);
    statementWrapper.setLocalTime(4, null);
    assertEquals(1, statement.executeUpdate());
    ResultSet resultSet = connection.createStatement().executeQuery("select * from test1");
    assertTrue(resultSet.next());
    return new ResultSetWrapper(metaData, resultSet);
  }
}
