package com.chariotsolutions.jshepard.sql;

import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class StreamTest {

  @After
  public void tearDown() {
    clearDatabase();
  }

  @Test
  public void testStream() throws Exception {
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
      insert(statement, 4, null, null, null);

      ResultSet resultSet = connection.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY).executeQuery("select * from test1");
      ResultSetWrapper resultSetWrapper = new ResultSetWrapper(connection.getMetaData(), resultSet);
      List<Integer> sortedIds = resultSetWrapper
          .stream()
          .filter(row -> getTime(row) != null)
          .sorted(Comparator.comparing(this::getTime))
          .map(row -> getId(row))
          .collect(Collectors.toList());
      assertEquals(2, sortedIds.get(0).intValue());
      assertEquals(1, sortedIds.get(1).intValue());
      assertEquals(3, sortedIds.get(2).intValue());
    }
  }

  private Integer getId(Object[] row) {
    return (Integer)row[0];
  }

  private LocalTime getTime(Object[] row) {
    return row[3] == null ? null : ((Time)row[3]).toLocalTime();
  }

  private void insert(PreparedStatement statement, int id, LocalDateTime localDateTime, LocalDate localDate, LocalTime localTime) throws SQLException {
    PreparedStatementWrapper statementWrapper = new PreparedStatementWrapper(statement);
    statement.clearParameters();
    statement.setInt(1, id);
    statementWrapper.setLocalDateTime(2, localDateTime);
    statementWrapper.setLocalDate(3, localDate);
    statementWrapper.setLocalTime(4, localTime);
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
