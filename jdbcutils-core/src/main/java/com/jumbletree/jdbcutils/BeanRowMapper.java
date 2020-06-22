package com.jumbletree.jdbcutils;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.persistence.Column;

import org.springframework.jdbc.core.RowMapper;

public class BeanRowMapper<T> implements RowMapper<T> {

	@FunctionalInterface
	public static interface Handler<T> {
		void handle(T t, Method setter, ResultSet rs, String column) throws Exception;
	}
	
	protected Class<T> clazz;
	private HashMap<String, Method> mappings = new HashMap<String, Method>();
	private HashMap<String, Handler<T>> handlers = new HashMap<>();

	/**
	 * Subclassing constructor.
	 * 
	 * Calling classes MUST call init(JDBCUtils) explicitly.
	 * 
	 * @param clazz
	 * @param utils
	 */
	protected BeanRowMapper(Class<T> clazz) {
		this.clazz = clazz;
	}
	
	public BeanRowMapper(Class<T> clazz, JDBCUtils utils) {
		this.clazz = clazz;
		init(utils);
	}

	protected void init(JDBCUtils utils) {
		for (Field field : utils.getAllFields(clazz)) {
			Column col = field.getAnnotation(Column.class);
			if (col != null) {
				Method setter = utils.getSetter(field);
				if (setter == null) {
					//Configuration error - has been logged
					continue;
				}

				//Now work out the function
				Class<?> type = setter.getParameterTypes()[0];
				Handler<T> handler = getExtensionHandler(field);
				if (handler == null) {
					if (type.equals(String.class)) {
						handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getString(column));
					//Consider java.util.Date to be a timestamp because this is common usage
					} else if (type.equals(Timestamp.class) || type.equals(Date.class)) {
						handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getTimestamp(column));
					//But java.sql.Date to be a date
					} else if (Date.class.isAssignableFrom(type)) {
						handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getDate(column));
					} else if (type.isEnum()) {
						handler = (t, theMethod, rs, column) -> theMethod.invoke(t, type.getMethod("valueOf", String.class).invoke(null,  rs.getString(column)));
					} else {
						if (type.equals(Integer.class) || type.equals(int.class)) {
							handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getInt(column));
						} else if (type.equals(Float.class) || type.equals(float.class)) {
							handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getFloat(column));
						} else if (type.equals(Double.class) || type.equals(double.class)) {
							handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getDouble(column));
						} else if (type.equals(BigDecimal.class)) {
                                                        handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getBigDecimal(column));
                                                } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
							handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getBoolean(column));
						} else if (type.equals(Byte[].class) || type.equals(byte[].class)) {
							handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getBytes(column));
						} else if (type.equals(Long.class) || type.equals(long.class)) {
							handler = (t, theMethod, rs, column) -> theMethod.invoke(t, rs.getLong(column));
						}
					}
				}

				if (handler == null) {
					throw new IllegalArgumentException("No handler found for type " + type.getName());
				}
				String name = col.name() == null || col.name().length() == 0 ? field.getName() : col.name();
				
				mappings.put(name, setter);
				handlers.put(name, handler);
			}
		}
	}

	/** 
	 * Subclasses can implement specific type handling here
	 * @param type the type that is being handled
	 */
	protected Handler<T> getExtensionHandler(Field type) {
		return null;
	}

	@Override
	public T mapRow(ResultSet rs, int row) throws SQLException {
		try {
			T t = clazz.newInstance();
			
			//Allow ordering in case marshalling requires it
			Collection<String> keys = setMappingOrder(mappings.keySet());
			
			for (String key : keys) {
				Method theMethod = mappings.get(key);

				Handler<T> handler = handlers.get(key);
				handler.handle(t, theMethod, rs, key);
				//Override for nulls...
				if (rs.wasNull()) try {
					theMethod.invoke(t, new Object[] {null});
				} catch (Exception e) {
					//Was a primitive, so leave as is.
				}
			}
			return t;
		} catch (Exception e) {
			System.out.println("**** Got an error");
			e.printStackTrace();
			Logger.getGlobal().log(Level.SEVERE, e.getMessage(), e);
			throw new SQLException(e);
		}
		
	}

	protected Collection<String> setMappingOrder(Collection<String> fields) {
		return fields;
	}
}
