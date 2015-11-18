package com.chariotsolutions.jshepard.sql;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.junit.Assert.*;

public class Jdbc42Test extends BaseJdbcTest {
  // Derby time fields do not support nanoseconds.
  LocalTime time = LocalTime.of(12, 34, 56);

  @After
  public void tearDown() {
    clearDatabase();
  }

  @Test
  public void testVersion() throws Exception {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals(4, metaData.getJDBCMajorVersion());
      assertEquals(2, metaData.getJDBCMinorVersion());
    }
  }

  @Test
  public void testLocalDateTimeByName() throws Exception {
    try (Connection connection = getConnection()) {
      LocalDateTime now = LocalDateTime.now();
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapper(connection, now);
      assertEquals(now, resultSetWrapper.getLocalDateTime("test1"));
    }
  }

  @Test
  public void testLocalDateTimeByIndex() throws Exception {
    try (Connection connection = getConnection()) {
      LocalDateTime now = LocalDateTime.now();
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapper(connection, now);
      assertEquals(now, resultSetWrapper.getLocalDateTime(2));
    }
  }

  @Test
  public void testLocalDateByName() throws Exception {
    try (Connection connection = getConnection()) {
      LocalDateTime now = LocalDateTime.now();
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapper(connection, now);
      assertEquals(date, resultSetWrapper.getLocalDate("test2"));
    }
  }

  @Test
  public void testLocalDateByIndex() throws Exception {
    try (Connection connection = getConnection()) {
      LocalDateTime now = LocalDateTime.now();
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapper(connection, now);
      assertEquals(date, resultSetWrapper.getLocalDate(3));
    }
  }

  @Test
  public void testLocalTimeByName() throws Exception {
    try (Connection connection = getConnection()) {
      LocalDateTime now = LocalDateTime.now();
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapper(connection, now);
      assertEquals(time, resultSetWrapper.getLocalTime("test3"));
    }
  }

  @Test
  public void testLocalTimeByIndex() throws Exception {
    try (Connection connection = getConnection()) {
      LocalDateTime now = LocalDateTime.now();
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapper(connection, now);
      assertEquals(time, resultSetWrapper.getLocalTime(4));
    }
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
