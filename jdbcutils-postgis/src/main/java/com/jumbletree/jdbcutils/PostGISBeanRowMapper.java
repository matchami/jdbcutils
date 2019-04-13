package com.jumbletree.jdbcutils;

import java.lang.reflect.Field;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.postgis.PGgeometry;
import org.springframework.jdbc.core.RowMapper;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class PostGISBeanRowMapper<T> extends BeanRowMapper<T> implements RowMapper<T> {

	private WKTReader reader = new WKTReader();
	
	public PostGISBeanRowMapper(Class<T> clazz, JDBCUtils utils) {
		super(clazz, utils);
	}

	@Override
	protected Handler<T> getExtensionHandler(Field field) {
		Class<?> type = field.getType();
		if (org.postgis.Geometry.class.isAssignableFrom(type) || Geometry.class.isAssignableFrom(type)) {
			return (t, theMethod, rs, column) -> {
				PGgeometry geom = (PGgeometry)rs.getObject(column);
				if (Geometry.class.isAssignableFrom(type)) {
					StringBuffer buffer = new StringBuffer();
					geom.getGeometry().outerWKT(buffer);
	
					try {
						Geometry jtsgeom = reader.read(buffer.toString());
						jtsgeom.setSRID(geom.getGeometry().getSrid());
						
						theMethod.invoke(t, jtsgeom);
					} catch (ParseException e) {
						//Not possible, so long as postgis and jts both work
						Logger.getLogger(getClass().getName()).log(Level.SEVERE, "Error creating geometry", e);
						throw e;
					} catch (NullPointerException e) {
						System.out.println("NPE on " + buffer.toString());
						throw e;
					}
				} else {
					theMethod.invoke(t, geom.getGeometry());
				}
			};
		}
		return null;
	}

}
