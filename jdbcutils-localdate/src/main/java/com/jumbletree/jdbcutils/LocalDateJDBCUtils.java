package com.jumbletree.jdbcutils;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

public class LocalDateJDBCUtils extends JDBCUtils {

	public LocalDateJDBCUtils(JdbcTemplate template) {
		super(template);
	}
	
	public LocalDateJDBCUtils(JdbcTemplate readTemplate, JdbcTemplate writeTemplate) {
		super(readTemplate, writeTemplate);
	}
	
	/**
	 * Creates a row mapper used to convert DB data to the object model.
	 * @param clazz the class which is to be created
	 */
	public <T> RowMapper<T> getRowMapper(Class<T> clazz) {
		return new LocalDateBeanRowMapper<>(clazz, this);
	}


}
