package com.jumbletree.jdbcutils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;

public class JDBCUtils {

	private final Logger logger = LoggerFactory.getLogger(JDBCUtils.class);
	
	private HashMap<Class<?>, Field> idCache = new HashMap<>();

	private JdbcTemplate readTemplate;
	private JdbcTemplate writeTemplate;
	
	public JDBCUtils(JdbcTemplate template) {
		this(template, template);
	}
	
	public JDBCUtils(JdbcTemplate readTemplate, JdbcTemplate writeTemplate) {
		this.readTemplate = readTemplate;
		this.writeTemplate = writeTemplate;
	}
	
	private ManyToMany findManyToMany(Class<?> clazz, Class<?> target) {
		for (Method method : getAllMethods(clazz)) {
			ManyToMany m2m = method.getAnnotation(ManyToMany.class);
			if (m2m != null && m2m.targetEntity().equals(target)) {
				return m2m;
			}
		}
		return null;
	}
	
	private ManyToMany findManyToMany(Class<?> clazz, Class<?> target, String property) {
		for (Method method : getAllMethods(clazz)) {
			ManyToMany m2m = method.getAnnotation(ManyToMany.class);
			if (m2m != null && m2m.targetEntity().equals(target) && method.getName().equals(getGetterName(property, false))) {
				return m2m;
			}
		}
		return null;
	}
	/**
	 * Returns a list of objects associated with another via a many-to-many relationship.  The
	 * relationship must be defined on the source class
	 * @param source the originating object
	 * @param targetClass the class of the remote object
	 * @param jdbcTemplate db access
	 * @return
	 */
	public <T> List<T> getManyToMany(Object source, Class<T> targetClass) {
		Class<?> sourceClass = source.getClass();
		
		ManyToMany sourceM2m = findManyToMany(sourceClass, targetClass);
		ManyToMany targetM2m = findManyToMany(targetClass, sourceClass);
		
		if (sourceM2m == null || targetM2m == null) {
			throw new IllegalArgumentException("Both " + sourceClass + " and " + targetClass + " must specify many to many details, but one or both does not");
		}
		
		String sourceMapped = sourceM2m.mappedBy();
		String mappingTable = sourceMapped.substring(0, sourceMapped.indexOf("."));
		String targetKey = sourceMapped.substring(sourceMapped.indexOf(".") + 1);
		
		String sourceKey = targetM2m.mappedBy();
		sourceKey = sourceKey.substring(sourceKey.indexOf(".") + 1);
		
		String query = "SELECT t.* FROM " + getTable(targetClass) + " t JOIN " + mappingTable + " m ON t."
				+ getIDColumn(targetClass) + " = m." + targetKey + " WHERE m." + sourceKey + " = ?";
		
		return readTemplate.query(query, getRowMapper(targetClass), getID(source));
	}
	
	/**
	 * Returns a list of objects associated with another via a many-to-many relationship.  The
	 * relationship must be defined on the source class.  This method enabled multiple many-to-many
	 * relationships to existing within the one class by allowing the specification of precisely 
	 * which relationship to use
	 * @param source the originating object
	 * @param targetClass the class of the remote object
	 * @param jdbcTemplate db access
	 * @return
	 */
	public <T> List<T> getManyToMany(Object source, Class<T> targetClass, String mappingFrom, String mappingTo) {
		Class<?> sourceClass = source.getClass();
		
		ManyToMany sourceM2m = findManyToMany(sourceClass, targetClass, mappingFrom);
		ManyToMany targetM2m = findManyToMany(targetClass, sourceClass, mappingTo);
		
		if (sourceM2m == null || targetM2m == null) {
			throw new IllegalArgumentException("Both " + sourceClass + " and " + targetClass + " must specify many to many details, but one or both does not");
		}
		
		String sourceMapped = sourceM2m.mappedBy();
		String mappingTable = sourceMapped.substring(0, sourceMapped.indexOf("."));
		String targetKey = sourceMapped.substring(sourceMapped.indexOf(".") + 1);
		
		String sourceKey = targetM2m.mappedBy();
		sourceKey = sourceKey.substring(sourceKey.indexOf(".") + 1);
		
		String query = "SELECT t.* FROM " + getTable(targetClass) + " t JOIN " + mappingTable + " m ON t."
				+ getIDColumn(targetClass) + " = m." + targetKey + " WHERE m." + sourceKey + " = ?";
		
		return readTemplate.query(query, getRowMapper(targetClass), getID(source));
	}

	/**
	 * Returns a list of objects associated with another via a one-to-many relationship.  The
	 * relationship must be defined on the source class and there must be only ONE relationship of the given target type.
	 * If multiple relationships exist, use getOneToMany(Object, Class<T>, String, JdbcTemplate) instead.
	 * @param source the originating object
	 * @param targetClass the class of the remote object
	 * @param jdbcTemplate db access
	 * @return
	 */
	public <T> List<T> getOneToMany(Object source, Class<T> targetClass) {
		Class<? extends Object> sourceClass = source.getClass();
		
		//Do a reverse lookup
		for (Field field : getAllFields(targetClass)) {
			ManyToOne m2o = field.getAnnotation(ManyToOne.class);
			if (m2o != null && m2o.targetEntity().equals(sourceClass)) {
				String targetTable = getTable(targetClass);
				//Now construct a query 

				String targetKey = getColumnName(field, null);
				
				String query = "SELECT t.* FROM " + targetTable + " t WHERE t." + targetKey + " = ?";
				return readTemplate.query(query, getRowMapper(targetClass), getID(source));
			}
		}
		throw new IllegalArgumentException(sourceClass + " does not define a one to many relationship for " + targetClass.getName());
	}

	/**
	 * Returns a list of objects associated with another via a one-to-many relationship on the given field
	 * @param source the originating object
	 * @param targetClass the class of the remote object
	 * @param mappedBy the field that holds the foreign key (in the foreign class)
	 * @param jdbcTemplate db access
	 * @return
	 */
	public <T> List<T> getOneToMany(Object source, Class<T> targetClass, String mappedBy) {
		Class<? extends Object> sourceClass = source.getClass();
		
		try {
			Field field = targetClass.getField(mappedBy);
			ManyToOne m2o = field.getAnnotation(ManyToOne.class);
			if (m2o == null || !m2o.targetEntity().equals(sourceClass)) {
				throw new IllegalArgumentException(targetClass + "." + mappedBy + " does not describe a many to one of type " + source.getClass());
			}
			String targetTable = getTable(targetClass);
			//Now construct a query 

			String targetKey = getColumnName(field, null);
			
			String query = "SELECT t.* FROM " + targetTable + " t WHERE t." + targetKey + " = ?";
			return readTemplate.query(query, getRowMapper(targetClass), getID(source));
		} catch (NoSuchFieldException | SecurityException e) {
			throw new IllegalArgumentException(targetClass + "." + mappedBy + " does not exist or is not accessible");
		}
	}

	/**
	 * Returns an objects associated with another via a many-to-one relationship.  The
	 * relationship must be defined on the source class and there must be only ONE relationship of the given target type.
	 * If multiple relationships exist, use getManyToOne(Object, Class<T>, String, JdbcTemplate) instead.
	 * @param source the originating object
	 * @param targetClass the class of the remote object
	 * @param jdbcTemplate db access
	 * @return
	 */
	public <T> T getManyToOne(Object source, Class<T> targetClass) {
		Class<? extends Object> sourceClass = source.getClass();
		
		//Need to do it by field
		for (Field field : getAllFields(sourceClass)) {
			ManyToOne m2o = field.getAnnotation(ManyToOne.class); 
			if (m2o != null && m2o.targetEntity().equals(targetClass)) {
				int fk = 0;
				try {
					fk = (Integer)getGetter(field).invoke(source);
				} catch (Exception e) {
					//Misconfigured...
					logger.error(e.getMessage(), e);
				}
				
				return get(targetClass, fk);
			}
		}
		
		throw new IllegalArgumentException(sourceClass + " does not define a one to many relationship for " + targetClass.getName());
	}

	public <T> T getManyToOne(Object source, Class<T> targetClass, String mappedBy) {
		Class<? extends Object> sourceClass = source.getClass();
		
		Field field;
		try {
			field = sourceClass.getField(mappedBy);

			ManyToOne m2o = field.getAnnotation(ManyToOne.class); 
			if (m2o == null || !m2o.targetEntity().equals(targetClass)) {
				throw new IllegalArgumentException(sourceClass + "." + mappedBy + " does not define a one to many relationship for " + targetClass.getName());
			}
			int fk = 0;
			try {
				fk = (Integer)getGetter(field).invoke(source);
			} catch (Exception e) {
				//Misconfigured...
				logger.error(e.getMessage(), e);
			}
			
			return get(targetClass, fk);
		} catch (NoSuchFieldException | SecurityException e1) {
			throw new IllegalArgumentException(sourceClass + "." + mappedBy + " does not exist or is not accesible for " + targetClass.getName());
		}
		
	}

//	private Field getField(Class<?> clazz, String column) {
//		for (Field field : getAllFields(clazz)) {
//			Column col = field.getAnnotation(Column.class);
//			if (col !=null && col.name().equals(column)) {
//				return field;
//			}
//		}
//		return null;
//	}

	
	protected String getTable(Class<?> clazz) {
		Table table = clazz.getAnnotation(Table.class);
		if (table != null && table.name() != null && table.name().length() > 0) 
			return table.name();
		
		Entity entity = clazz.getAnnotation(Entity.class);
		if (entity != null && entity.name() != null && entity.name().length() > 0) 
			return entity.name();
		
		throw new IllegalArgumentException("No table name specified");
	}
	
	protected String getIDColumn(Class<?> clazz) {
		Field id = getIDField(clazz);
		if (id == null) {
			throw new IllegalArgumentException(clazz.getName() + " does not specify an id field");
		}
		try {
			return id.getAnnotation(Column.class).name();
		} catch (NullPointerException e) {
			throw new IllegalArgumentException(clazz.getName() + "'s id field does not specify a column");
		}
	}
	
	private Serializable getID(Object source) {
		Field id = getIDField(source.getClass());
		try {
			return (Serializable)getGetter(id).invoke(source);
		} catch (IllegalAccessException | NoSuchMethodException | SecurityException | InvocationTargetException e) {
			throw new IllegalArgumentException("Couldn't access id of " + source.getClass().getName());
		}
	}
	
	protected Method getGetter(Field field) throws NoSuchMethodException, SecurityException {
		String name = field.getName();
		name = getGetterName(name, field.getType().equals(Boolean.TYPE));
		return field.getDeclaringClass().getMethod(name);
	}

	private String getGetterName(String name, boolean isBoolean) {
		if (isBoolean) {
			name = "is" + name.substring(0, 1).toUpperCase() + name.substring(1);
		} else {
			name = "get" + name.substring(0, 1).toUpperCase() + name.substring(1);
		}
		return name;
	}
	
	protected Field getIDField(Class<?> clazz) {
		Field id = idCache.get(clazz);
		if (id == null) {
			for (Field field : clazz.getDeclaredFields()) {
				if (field.getAnnotation(Id.class) != null) {
					id = field;
					idCache.put(clazz, id);
				}
			}
		}
		return id;
	}

	public Method getSetter(Field field) {
		String name = field.getName();
		Class<?> type = field.getType();
		
		try {
			return field.getDeclaringClass().getMethod("set" + name.substring(0, 1).toUpperCase() + name.substring(1), type);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			return null;
		}
	}
	
	public void insertOrUpdate(Object o) {

	    Serializable id = getID(o);
	    if (id instanceof Integer) {
	        int intID = (Integer)id;
    		if (intID <= 0) {
    			insert(o);
    		} else {
    			update(o);
    		}
	    } else {
	        if (id == null)
	            insert(o);
	        else 
	            update(o);
	    }
	}
	
	public void delete(Object o) {
		Class<?> clazz = o.getClass();
		String query = "DELETE FROM " + getTable(clazz) + " WHERE " + getIDColumn(clazz) +
				" = ?";
		
		writeTemplate.update(query, getID(o));
	}
	
	public <T> void update(T o) {
		StringBuilder query = new StringBuilder();
		
		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>)o.getClass();
		
		final List<Object> values = new ArrayList<>();
		final List<Class<?>> types = new ArrayList<>();
		Field idField = null;
		
		for (Field field : getAllFields(clazz)) {
			if (field.getAnnotation(Id.class) != null) {
				idField = field;
				continue;
			}
			Column col = field.getAnnotation(Column.class);
			
			if (col == null)
				continue;
			
			Object value = null;
			try {
				value = getPersistenceObject(field, o, true);
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				//Shouldn't ever happen - null is OK if it does
			}

			query.append(",").append(getColumnName(field, col)).append(" = ?");
			values.add(value);
			types.add(field.getType());
		}
		
		final String sql = "UPDATE " + getTable(clazz) + " SET "
				+ query.substring(1) + " WHERE " + getColumnName(idField, null) + " = ?";

		try {
			values.add(getGetter(idField).invoke(o));
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			//Misconfigured class
			logger.error("Misconfigured class", e);
		}
		Object[] valArray = values.toArray();
		
		writeTemplate.update(sql, valArray);
		
		ObjectCache cache = ObjectCache.get();
		if (cache.get(clazz, getID(o)) != o) {
			cache.set(clazz, getID(o), o);
		}
	}

	public <T> void cautiousUpdate(T o) {
		StringBuilder query = new StringBuilder();
		
		@SuppressWarnings("unchecked")
		Class<T> clazz = (Class<T>)o.getClass();
		
		final List<Object> values = new ArrayList<>();
		final List<Class<?>> types = new ArrayList<>();
		Field idField = null;
		
		for (Field field : getAllFields(clazz)) {
			if (field.getAnnotation(Id.class) != null) {
				idField = field;
				break;
			}
		}
		
		T existingObject = get(clazz, ((Integer)getID(o)).intValue(), false);

		for (Field field : getAllFields(clazz)) {
			if (field.getAnnotation(Id.class) != null) {
				continue;
			}
			Column col = field.getAnnotation(Column.class);
			
			if (col == null)
				continue;
			
			Object value = null;
			try {
				Object existingValue = getPersistenceObject(field, existingObject, false);
				value = getPersistenceObject(field, o, true);
				if (nullSafeEquals(value, existingValue))
					continue;
				
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				//Shouldn't ever happen - null is OK if it does
			}

			query.append(",").append(getColumnName(field, col)).append(" = ?");
			values.add(value);
			types.add(field.getType());
		}
		
		//Check if there were actual updates
		if (query.length() > 0) {
		
			final String sql = "UPDATE " + getTable(clazz) + " SET "
					+ query.substring(1) + " WHERE " + getColumnName(idField, null) + " = ?";
	
			try {
				values.add(getGetter(idField).invoke(o));
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				//Misconfigured class
				logger.error("Misconfigured class", e);
			}
			Object[] valArray = values.toArray();
			
			writeTemplate.update(sql, valArray);
		}
		
		ObjectCache cache = ObjectCache.get();
		if (cache.get(clazz, getID(o)) != o) {
			cache.set(clazz, getID(o), o);
		}
	}


	private boolean nullSafeEquals(Object v1, Object v2) {
		if (v1 == null && v2 == null)
			return true;
		if (v1 == null ^ v2 == null)
			return false;
		return v1.equals(v2);
	}

	public void insert(Object o) {

		Class<?> clazz = o.getClass();
		
		Map<String, Object> fields = new HashMap<>();
		Field idField = null;
		
		for (Field field : getAllFields(clazz)) {
			if (field.getAnnotation(Id.class) != null && field.getAnnotation(GeneratedValue.class) != null ) {
				idField = field;
				continue;
			}
			Column col = field.getAnnotation(Column.class);
			if (col == null)
				continue;
		
			try {
				fields.put(getColumnName(field, col), getPersistenceObject(field, o, true));
				logger.debug("Putting " + getColumnName(field, col) + ": " +getGetter(field).invoke(o));
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
				//Shouldn't ever happen - null is OK if it does
				logger.error("Shouldn't ever happen - null is OK if it does", e);
			}
		}
		
		SimpleJdbcInsert insert = new SimpleJdbcInsert(writeTemplate);
		insert
			.withTableName(getTable(clazz))
			.usingColumns(fields.keySet().toArray(new String[fields.keySet().size()]));
		if (idField != null) {
			insert.usingGeneratedKeyColumns(getColumnName(idField, idField.getAnnotation(Column.class)));
			Number genKey = insert.executeAndReturnKey(fields);
			try {
				getSetter(idField).invoke(o, genKey.intValue());
			} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
				//Misconfigured class
				logger.error("Misconfigured class", e);
			}
		} else {
			insert.execute(fields);
		}
	}
	
	/**
	 * A cautious update will look for data on the existing object as well as the saving object.
	 * @param forProcessing informs this method that the request should NOT be considered as a serialisation request
	 */
	protected Object getPersistenceObject(Field field, Object toPersist, boolean forProcessing) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		return getPersistenceObject(field, toPersist);
	}
	
	/**
	 * Use the getPersistenceObject(Field field, Object toPersist, boolean forProcessing) variant in preference
	 */
	@Deprecated
	protected Object getPersistenceObject(Field field, Object toPersist) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		if (field.getType().isEnum()) {
			return getGetter(field).invoke(toPersist).toString();
		}
		return getGetter(field).invoke(toPersist);
	}

	List<Field> getAllFields(Class<?> clazz) {
		List<Field> fieldsList = new ArrayList<Field>();
		Class<?> theClass = clazz;
		while (theClass != null) {
			for (Field field : theClass.getDeclaredFields()) {
				fieldsList.add(field);
			}
			theClass = theClass.getSuperclass();
		}
		return fieldsList;
	}

	List<Method> getAllMethods(Class<?> clazz) {
		List<Method> methodsList = new ArrayList<Method>();
		Class<?> theClass = clazz;
		while (theClass != null) {
			for (Method method : theClass.getDeclaredMethods()) {
				methodsList.add(method);
			}
			theClass = theClass.getSuperclass();
		}
		return methodsList;
	}

	private String getColumnName(Field field, Column col) {
		if (col == null)
			col = field.getAnnotation(Column.class);
		if (col == null)
			return null;
		
		if (col.name() != null && col.name().length() > 0) {
			return col.name(); 
		}
		
		return field.getName();
	}
	
	/**
	 * Creates a row mapper used to convert DB data to the object model.
	 * @param clazz the class which is to be created
	 */
	public <T> RowMapper<T> getRowMapper(Class<T> clazz) {
		return new BeanRowMapper<>(clazz, this);
	}

	public <T> T get(Class<T> clazz, int id) {
		return get(clazz, id, true);
	}
	public <T> T get(Class<T> clazz, int id, boolean useCache) {
		T t = useCache ? ObjectCache.get().get(clazz, id) : null;
		if (t == null) {
			t = readTemplate.queryForObject("SELECT * FROM " + getTable(clazz) + " WHERE " + getIDColumn(clazz) + " = ?", getRowMapper(clazz), id);
			if (useCache) 
				ObjectCache.get().set(clazz, id, t);
		}
		return t;
	}
	
	/**
	 * Creates a query that will find all of the objects represented by targetClass where
	 * foreign keys exist to the given constraint objects.  Results are only guaranteed
	 * if the target class has exactly ONE foreign key to each given object.
	 * 
	 * @param clazz
	 * @param constraints
	 * @return
	 */
	public <T> List<T> queryFor(Class<T> targetClass, Object ... constraints) {
		StringBuffer query = new StringBuffer().append("SELECT * FROM ").append(getTable(targetClass)).append(" WHERE ");
		
		Object[] ids = new Object[constraints.length];
		outer: for (int i=0; i<constraints.length; i++) {
			Object o = constraints[i];
			Class<?> constraintClass = o.getClass();
			for (Field field : getAllFields(targetClass)) {
				ManyToOne m2o = field.getAnnotation(ManyToOne.class); 
				if (m2o != null && m2o.targetEntity().equals(constraintClass)) {
					
					query.append(getColumnName(field, null)).append(" = ?");
					ids[i] = getID(o);
					if (i < constraints.length-1) {
						query.append(" AND ");
					}
					continue outer;
				}
			}
			throw new IllegalArgumentException(targetClass + " does not specify a many-to-one join with " + constraintClass);
		}
		return readTemplate.query(query.toString(), getRowMapper(targetClass), ids);
	}
	
	public void removeManyToMany(Object o1, Object o2) {
		Class<?> sourceClass = o1.getClass();
		Class<?> targetClass = o2.getClass();
		
		ManyToMany sourceM2m = findManyToMany(sourceClass, targetClass);
		ManyToMany targetM2m = findManyToMany(targetClass, sourceClass);
		
		String sourceMapped = sourceM2m.mappedBy();
		String mappingTable = sourceMapped.substring(0, sourceMapped.indexOf("."));
		String targetKey = sourceMapped.substring(sourceMapped.indexOf(".") + 1);
		
		String sourceKey = targetM2m.mappedBy();
		sourceKey = sourceKey.substring(sourceKey.indexOf(".") + 1);

		String query = "DELETE FROM " + mappingTable + " WHERE " + targetKey + " = ? AND " + sourceKey + " = ?";

		writeTemplate.update(query, getID(o2), getID(o1));
		
	}
	
	public void addManyToMany(Object o1, Object o2) {
		Class<?> sourceClass = o1.getClass();
		Class<?> targetClass = o2.getClass();
		
		ManyToMany sourceM2m = findManyToMany(sourceClass, targetClass);
		ManyToMany targetM2m = findManyToMany(targetClass, sourceClass);
		
		String sourceMapped = sourceM2m.mappedBy();
		String mappingTable = sourceMapped.substring(0, sourceMapped.indexOf("."));
		String targetKey = sourceMapped.substring(sourceMapped.indexOf(".") + 1);
		
		String sourceKey = targetM2m.mappedBy();
		sourceKey = sourceKey.substring(sourceKey.indexOf(".") + 1);

		String query = "INSERT INTO " + mappingTable + "(" + sourceKey + ", " + targetKey + ") VALUES (?, ?)";

		writeTemplate.update(query, getID(o1), getID(o2));
	}

	public <T> List<T> getAll(Class<T> clazz) {
		List<T> list = readTemplate.query("SELECT * FROM " + getTable(clazz), getRowMapper(clazz));
		for (T t : list) 
			ObjectCache.get().set(clazz, getID(t), t);

		return list;
	}
	
	public void close() {
		ObjectCache.terminate();
	}
}
