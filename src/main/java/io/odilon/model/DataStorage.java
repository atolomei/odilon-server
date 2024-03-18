/*
 * Odilon Object Storage
 * (C) Novamens 
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.odilon.model;


import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import io.odilon.log.Logger;
import io.odilon.util.Check;

/**
 * <p>Data Storage modes</p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public enum DataStorage {
	
	READ_WRITE	("rw", 0),
	READONLY 	("ro", 1),
	WORM		("worm", 2);
		
	
	@SuppressWarnings("unused")
	private static Logger logger = Logger.getLogger(DataStorage.class.getName());

	private static List<DataStorage> ds;
	private static List<String> names;

	private String name;
	private int code;
	
	public String getDescription() {
		return getDescription(Locale.getDefault());
	}
	
	public String getDescription(Locale locale) {
		ResourceBundle res = ResourceBundle.getBundle(DataStorage.this.getClass().getName(), locale);
		return res.getString(this.getName());
	}
	
	public String toJSON() {
		StringBuilder str = new StringBuilder();
		str.append("\"name\":\"" + name + "\"");
		str.append(", \"code\":" + code );
		str.append(", \"description\": \"" + getDescription() + "\"");
		return str.toString();
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		str.append(this.getClass().getSimpleName() +"{");
		str.append(toJSON());
		str.append("}");
		return str.toString();
	}
	
	public String getName() {
		return name;
	}
	
	public int getCode() {
		return code;
	}
	
	private DataStorage(String name, int code) {
		this.name = name;
		this.code = code;
	}
	
	
	public static List<String> getNames() {
		
		if (names!=null)
			return names;
		
		synchronized (DataStorage.class) {
			names  = new ArrayList<String>();
			names.add( READ_WRITE.getName());
			names.add( READONLY.getName());
			names.add( WORM.getName());
		}
		return names;
	}
	public static List<DataStorage> getValues() {

		if (ds!=null)
			return ds;
		
		synchronized (DataStorage.class) {
			ds = new ArrayList<DataStorage>();
			ds.add( READ_WRITE);
			ds.add( READONLY);
			ds.add( WORM);
		}
		return ds;
	}
	
	
	public static DataStorage fromId(String id) {
		
		Check.requireNonNullArgument(id, "id is null");
			
		try {
				int value = Integer.valueOf(id).intValue();
				return fromCode(value);
					
		} catch (IllegalArgumentException e) {
			throw (e);
		}	
		catch (Exception e) {
			throw new IllegalArgumentException("id not integer -> " + id);
		}
	}
		
	/**
	 * @param name
	 * @return
	 */
	public static DataStorage fromString(String name) {

		Check.requireNonNullArgument(name, "name is null");

		String normalized = name.toLowerCase().trim();
		
		if (normalized.equals(READ_WRITE.getName())) 	return READ_WRITE;
		if (normalized.equals(READONLY.getName())) 		return READONLY;
		if (normalized.equals(WORM.getName())) 			return WORM;
		
		throw new IllegalArgumentException ("unsuported name -> " + name);
	}
	
	public static DataStorage fromCode(int code) {
		
		if (code==READ_WRITE.getCode()) return READ_WRITE;
		if (code== READONLY.getCode()) 	return READONLY;
		if (code==WORM.getCode()) 		return WORM;
		
		throw new IllegalArgumentException ("unsuported code -> " + String.valueOf(code));
	}


}
