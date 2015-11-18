package com.chariotsolutions.jshepard.sql;

import org.apache.commons.dbutils.ResultSetIterator;

import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class ResultSetWrapper {
  private final DatabaseMetaData databaseMetaData;
  private final ResultSet resultSet;

  public ResultSetWrapper(DatabaseMetaData databaseMetaData, ResultSet resultSet) {
    this.databaseMetaData = databaseMetaData;
    this.resultSet = resultSet;
  }

  public boolean supportsJavaTime() throws SQLException {
    return databaseMetaData.getJDBCMajorVersion() >= 5 ||
        (databaseMetaData.getJDBCMajorVersion() == 4 && databaseMetaData.getJDBCMinorVersion() >= 2);
  }

  private void checkSupportsJavaTime() throws SQLException {
    if (!supportsJavaTime()) {
      throw new SQLException(String.format("JDBC driver must be at least version 4.2, but is version %d.%d",
          databaseMetaData.getJDBCMajorVersion(), databaseMetaData.getJDBCMinorVersion()));
    }
  }

  public Stream<Object[]> stream() {
    return StreamSupport
        .stream(
            Spliterators
                .spliteratorUnknownSize(
                    new ResultSetIterator(resultSet), 0),
            false);
  }

  public LocalDate getLocalDate(int columnIndex) throws SQLException {
    // At Java One 2015, it was mentioned, that
    // Optional.ofNullable is not a valid
    // replacement for ?:, but I think
    // in this case, it is better than calling
    // getDate twice.
    return Optional.ofNullable(resultSet.getDate(columnIndex)).map(Date::toLocalDate).orElse(null);
  }

  public LocalDate getLocalDate(String columnLabel) throws SQLException {
    return Optional.ofNullable(resultSet.getDate(columnLabel)).map(Date::toLocalDate).orElse(null);
  }

  public void setLocalDate(int columnIndex, LocalDate value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnIndex, value, JDBCType.DATE);
  }

  public void setLocalDate(String columnLabel, LocalDate value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnLabel, value, JDBCType.DATE);
  }

  public LocalTime getLocalTime(int columnIndex) throws SQLException {
    // Can't use getTime because it is missing nanoseconds.
    return Optional.ofNullable(resultSet.getTimestamp(columnIndex))
        .map(Timestamp::toLocalDateTime)
        .map(LocalDateTime::toLocalTime)
        .orElse(null);
  }

  public LocalTime getLocalTime(String columnLabel) throws SQLException {
    // Can't use getTime because it is missing nanoseconds.
    return Optional.ofNullable(resultSet.getTimestamp(columnLabel))
        .map(Timestamp::toLocalDateTime)
        .map(LocalDateTime::toLocalTime)
        .orElse(null);
  }

  public void setLocalTime(int columnIndex, LocalTime value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnIndex, value, JDBCType.TIME);
  }

  public void setLocalTime(String columnLabel, LocalTime value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnLabel, value, JDBCType.TIME);
  }

  public LocalDateTime getLocalDateTime(int columnIndex) throws SQLException {
    return Optional.ofNullable(resultSet.getTimestamp(columnIndex)).map(Timestamp::toLocalDateTime).orElse(null);
  }

  public LocalDateTime getLocalDateTime(String columnLabel) throws SQLException {
    return Optional.ofNullable(resultSet.getTimestamp(columnLabel)).map(Timestamp::toLocalDateTime).orElse(null);
  }

  public void setLocalDateTime(int columnIndex, LocalDateTime value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnIndex, value, JDBCType.TIMESTAMP);
  }

  public void setLocalDateTime(String columnLabel, LocalDateTime value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnLabel, value, JDBCType.TIMESTAMP);
  }

  public OffsetDateTime getOffsetDateTime(int columnIndex, ZoneOffset zoneOffset) throws SQLException {
    return Optional.ofNullable(resultSet.getTimestamp(columnIndex))
        .map(Timestamp::toLocalDateTime)
        .map(localDateTime -> localDateTime.atOffset(zoneOffset))
        .orElse(null);
  }

  public OffsetDateTime getOffsetDateTime(String columnLabel, ZoneOffset zoneOffset) throws SQLException {
    return Optional.ofNullable(resultSet.getTimestamp(columnLabel))
        .map(Timestamp::toLocalDateTime)
        .map(localDateTime -> localDateTime.atOffset(zoneOffset))
        .orElse(null);
  }

  public void setOffsetDateTime(int columnIndex, OffsetDateTime value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnIndex, value, JDBCType.TIMESTAMP_WITH_TIMEZONE);
  }

  public void setOffsetDateTime(String columnLabel, OffsetDateTime value) throws SQLException {
    checkSupportsJavaTime();
    resultSet.updateObject(columnLabel, value, JDBCType.TIMESTAMP_WITH_TIMEZONE);
  }
}
