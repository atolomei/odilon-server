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

import io.odilon.util.Check;

public enum ServiceStatus {

	STOPPED 	("stopped", 0),
	STARTING 	("starting", 1),
	RUNNING 	("running", 2), 
	STOPPING 	("stopping", 3);
		
	private static List<ServiceStatus> ops;
	
	private String name;
	private int code;
	
	public String getDescription() {
		return getDescription(Locale.getDefault());
	}
	
	public String getDescription(Locale locale) {
		//ResourceBundle res = ResourceBundle.getBundle(getClass().getName(), locale);
		//return res.getString(this.getName());
		return this.getName();
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
		str.append(this.getClass().getSimpleName());
		str.append(toJSON());
		return str.toString();
	}
	
	public String getName() {
		return name;
	}
	
	public int getCode() {
		return code;
	}
	
	private ServiceStatus(String name, int code) {
		this.name = name;
		this.code = code;
	}
	
	public static List<ServiceStatus> getValues() {
		if (ops!=null)
			return ops;
		ops = new ArrayList<ServiceStatus>();
		ops.add( STOPPED);
		ops.add( STARTING);
		ops.add( RUNNING);
		ops.add( STOPPING);
		return ops;
	}
	
	
	public static ServiceStatus fromId(String id) {
		
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
	 * 
	 * @param name
	 * @return
	 */
	public static ServiceStatus fromString(String name) {
		
		Check.requireNonNullArgument(name, "id is null");
		
		String normalized = name.toUpperCase().trim();
		
		if (normalized.equals(STOPPED.getName())) return STOPPED;
		if (normalized.equals(STARTING.getName())) return STARTING;
		if (normalized.equals(RUNNING.getName())) return RUNNING;
		if (normalized.equals(STOPPING.getName())) return STOPPING;
		
		throw new IllegalArgumentException ("unsuported name -> " + name);
	}
	
	public static ServiceStatus  fromCode(int code) {
		
		if (code==STOPPED.getCode()) return STOPPED;
		if (code==STARTING.getCode()) return STARTING;
		if (code==RUNNING.getCode()) return RUNNING;
		if (code==STOPPING.getCode()) return STOPPING;
		
		throw new IllegalArgumentException ("unsuported code -> " + String.valueOf(code));
	}

}
