package com.jumbletree.jdbcutils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgis.PGgeometry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTWriter;

public class PostGISJDBCUtils extends JDBCUtils {

	private static WKTWriter writer = new WKTWriter();
	
	public PostGISJDBCUtils(JdbcTemplate template) {
		super(template);
	}
	
	/**
	 * Creates a row mapper used to convert DB data to the object model.
	 * @param clazz the class which is to be created
	 */
	public <T> RowMapper<T> getRowMapper(Class<T> clazz) {
		return new PostGISBeanRowMapper<>(clazz, this);
	}

	@Override
	protected Object getPersistenceObject(Field field, Object toPersist) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		Object o = super.getPersistenceObject(field, toPersist);
		
		if (o != null) {
			Class<?> theClass = o.getClass();
			if (Geometry.class.isAssignableFrom(theClass)) {
				Geometry jtsgeom = (Geometry)o;
				try {
					org.postgis.Geometry geometry = PGgeometry.geomFromString("SRID=" + jtsgeom.getSRID() + ";" + writer.write(jtsgeom));
					return new PGgeometry(geometry);
				} catch (SQLException e) {
					//Not possible if everything is working alright
					Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error creating geometry", e);
					return null;
				}
			} else if (org.postgis.Geometry.class.isAssignableFrom(theClass)) {
				return new PGgeometry((org.postgis.Geometry)o);
			}
		}
		return o;
	}

}
