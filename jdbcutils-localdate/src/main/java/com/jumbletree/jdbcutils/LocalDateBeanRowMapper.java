package com.jumbletree.jdbcutils;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.springframework.jdbc.core.RowMapper;

public class LocalDateBeanRowMapper<T> extends BeanRowMapper<T> implements RowMapper<T> {

	public LocalDateBeanRowMapper(Class<T> clazz, JDBCUtils utils) {
		super(clazz, utils);
	}

	@Override
	protected Handler<T> getExtensionHandler(Field field) {
		Class<?> type = field.getType();
		if (type.equals(LocalDate.class)) {
			return (t, theMethod, rs, column) -> {
				Date date = rs.getDate(column);
				theMethod.invoke(t, date == null ? null : date.toLocalDate());
			};
		} else if (type.equals(LocalDateTime.class)) {
			return (t, theMethod, rs, column) -> {
				Timestamp timestamp = rs.getTimestamp(column);
				theMethod.invoke(t, timestamp == null ? null : timestamp.toLocalDateTime());
			};
		}
		return null;
	}

}
