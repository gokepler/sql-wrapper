package com.chariotsolutions.jshepard.sql;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class PreparedStatementWrapper {
  private final PreparedStatement preparedStatement;

  public PreparedStatementWrapper(PreparedStatement preparedStatement) {
    this.preparedStatement = preparedStatement;
  }

  public void setLocalDateTime(int oneBasedIndex, LocalDateTime localDateTime) throws SQLException {
    preparedStatement.setTimestamp(oneBasedIndex, convert(localDateTime));
  }

  public void setLocalDate(int oneBasedIndex, LocalDate localDate) throws SQLException {
    preparedStatement.setDate(oneBasedIndex, convert(localDate));
  }

  public void setLocalTime(int oneBasedIndex, LocalTime localTime) throws SQLException {
    preparedStatement.setTimestamp(oneBasedIndex, convert(localTime));
  }

  /**
   * Convert a LocalDateTime to a Timestamp using the system timezone.
   */
  private Timestamp convert(LocalDateTime localDateTime) {
    return localDateTime == null ? null : Timestamp.valueOf(localDateTime);
  }

  private Date convert(LocalDate localDate) {
    return localDate == null ? null : Date.valueOf(localDate);
  }

  /**
   * Convert a LocalTime to a Timestamp. The LocalTime can't be converted
   * to a java.sql.Time because the nanoseconds will be lost.
   */
  private Timestamp convert(LocalTime localTime) {
    return localTime == null ? null : Timestamp.valueOf(localTime.atDate(LocalDate.ofEpochDay(0)));
  }
}
