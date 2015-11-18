package com.chariotsolutions.jshepard.sql;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

public class Jdbc40Test extends BaseJdbcTest {
  @Test
  public void testVersion() throws Exception {
    try (Connection connection = getConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();
      assertEquals(4, metaData.getJDBCMajorVersion());
      assertEquals(0, metaData.getJDBCMinorVersion());
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
  public void testLocalDateTimeByNameWithNull() throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapperWithNulls(connection);
      assertEquals(null, resultSetWrapper.getLocalDateTime("test1"));
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
  public void testLocalDateTimeByIndexWithNull() throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapperWithNulls(connection);
      assertEquals(null, resultSetWrapper.getLocalDateTime(2));
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
  public void testLocalDateByNameWithNull() throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapperWithNulls(connection);
      assertEquals(null, resultSetWrapper.getLocalDate("test2"));
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
  public void testLocalDateByIndexWithNull() throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapperWithNulls(connection);
      assertEquals(null, resultSetWrapper.getLocalDate(3));
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
  public void testLocalTimeByNameWithNull() throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapperWithNulls(connection);
      assertEquals(null, resultSetWrapper.getLocalTime("test3"));
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

  @Test
  public void testLocalTimeByIndexWithNull() throws Exception {
    try (Connection connection = getConnection()) {
      ResultSetWrapper resultSetWrapper = getTimestampResultSetWrapperWithNulls(connection);
      assertEquals(null, resultSetWrapper.getLocalTime(4));
    }
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection("jdbc:h2:mem:");
  }
}