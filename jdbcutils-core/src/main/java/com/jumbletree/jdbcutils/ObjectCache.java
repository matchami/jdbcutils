package com.jumbletree.jdbcutils;

import java.io.Serializable;
import java.util.HashMap;

public class ObjectCache extends HashMap<Class<?>, HashMap<Serializable, Object>> {

	private static final long serialVersionUID = 1L;

	private static ThreadLocal<ObjectCache> caches = new ThreadLocal<>();

	public static void create() {
		caches.set(new ObjectCache());
	}

	public static void terminate() {
		caches.remove();
	}
	
	public static ObjectCache get() {
		if (caches.get() == null) {
			create();
		}
		return caches.get();
	}
	
	@SuppressWarnings("unchecked")
	public <T> T get(Class<T> clazz, Serializable id) {
		HashMap<Serializable, Object> map = get(clazz);
		
		if (map != null) {
			return (T)map.get(id);
		}
		
		return null;
	}
	
	public <T> void set(Class<T> clazz, Serializable id, T obj) {
		HashMap<Serializable, Object> map = get(clazz);
		if (map == null) {
			map = new HashMap<>();
			put(clazz, map);
		}
		map.put(id, obj);
	}
}
