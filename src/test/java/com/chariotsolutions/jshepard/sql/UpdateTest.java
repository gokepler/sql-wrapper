package com.chariotsolutions.jshepard.sql;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;

import static org.junit.Assert.assertEquals;

/**
 * These tests should probably work with JDBC 4.2, but do not.
 */
public class UpdateTest {

  @After
  public void tearDown() {
    clearDatabase();
  }

  @Test
  public void fakeTest() {
    System.out.println("The tests are disabled. They can be re-enabled when java.time is supported in JDBC 4.2");
  }

//  @Test
  public void testResultSet() throws Exception {
    try (Connection connection = getConnection()) {
      connection.createStatement().execute("create table test1 (id int primary key, test1 timestamp, test2 date, test3 time)");
      ResultSet resultSet = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE).executeQuery("select * from test1");
      ResultSetWrapper resultSetWrapper = new ResultSetWrapper(connection.getMetaData(), resultSet);
      LocalDateTime dateTime1 = LocalDateTime.now();
      LocalDate date1 = LocalDate.of(2000, Month.JANUARY, 05);
      LocalTime time1 = LocalTime.of(12, 34, 56);
      resultSet.moveToInsertRow();
      resultSet.updateInt(1, 1);
      resultSetWrapper.setLocalDateTime(2, dateTime1);
      resultSetWrapper.setLocalDate(3, date1);
      resultSetWrapper.setLocalTime(4, time1);
      resultSet.insertRow();
      resultSet.close();
    }
  }

//  @Test
  public void testStatement() throws Exception {
    try (Connection connection = getConnection()) {
      connection.createStatement().execute("create table test1 (id int primary key, test1 timestamp, test2 date, test3 time)");
      PreparedStatement statement = connection.prepareStatement("insert into test1 values(?, ?, ?, ?)");
      LocalDateTime dateTime1 = LocalDateTime.now();
      LocalDate date1 = LocalDate.of(2000, Month.JANUARY, 05);
      LocalTime time1 = LocalTime.of(12, 34, 56);
      insert(statement, 1, dateTime1, date1, time1);
      LocalTime time2 = time1.withHour(1);
      insert(statement, 2, dateTime1, date1, time2);
      LocalTime time3 = time1.withHour(15);
      insert(statement, 3, dateTime1, date1, time3);
    }
  }

  private void insert(PreparedStatement statement, int id, LocalDateTime localDateTime, LocalDate localDate, LocalTime localTime) throws SQLException {
    statement.clearParameters();
    statement.setObject(1, id, JDBCType.INTEGER);
    statement.setObject(2, localDateTime, JDBCType.TIMESTAMP);
    statement.setObject(3, localDate, JDBCType.DATE);
    statement.setObject(4, localTime, JDBCType.TIME);
    assertEquals(1, statement.executeUpdate());
  }

  private void clearDatabase() {
    try {
      DriverManager.getConnection("jdbc:derby:memory:myDB;drop=true");
    } catch (Exception ignore) {
      // Always throws an exception.
    }
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:derby:memory:myDB;create=true");
  }
}
