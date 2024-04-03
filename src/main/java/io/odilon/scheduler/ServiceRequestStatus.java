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
package io.odilon.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public enum ServiceRequestStatus {

	STOPPED 	("stopped", 0),
	STARTING 	("starting", 1),
	RUNNING 	("running", 2),
	ERROR	 	("error", 3),
	STOPPING 	("stopping", 4),
	COMPLETED 	("completed", 5);
		
	private String name;
	private int code;

	static List<ServiceRequestStatus> ops;
	
	public static ServiceRequestStatus fromId(String id) {
		
	if (id==null)
		throw new IllegalArgumentException("id is null");
		
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
	public static List<ServiceRequestStatus> getValues() {
		
		if (ops!=null)
			return ops;
		
		ops = new ArrayList<ServiceRequestStatus>();
		
		ops.add( STOPPED);
		ops.add( STARTING);
		ops.add( RUNNING);
		ops.add( STOPPING);
		
		return ops;
	}
	
	/**
	 * 
	 * @param name
	 * @return
	 */
	public static ServiceRequestStatus fromString(String name) {
		
		if (name==null)
			throw new IllegalArgumentException ("name is null");
		
		String normalized = name.toUpperCase().trim();
		
		if (normalized.equals(STOPPED.getName())) return STOPPED;
		if (normalized.equals(STARTING.getName())) return STARTING;
		if (normalized.equals(RUNNING.getName())) return RUNNING;
		if (normalized.equals(STOPPING.getName())) return STOPPING;
		
		throw new IllegalArgumentException ("unsuported name -> " + name);
		
	}
	
	public static ServiceRequestStatus  fromCode(int code) {
		
		if (code==STOPPED.getCode()) return STOPPED;
		if (code==STARTING.getCode()) return STARTING;
		if (code==RUNNING.getCode()) return RUNNING;
		if (code==STOPPING.getCode()) return STOPPING;
		
		throw new IllegalArgumentException ("unsuported code -> " + String.valueOf(code));
	}
	
	public String getDescription() {
		return getDescription(Locale.getDefault());
	}
	
	public String getDescription(Locale locale) {
		ResourceBundle res = ResourceBundle.getBundle(ServiceRequestStatus.this.getClass().getName(), locale);
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
	
	private ServiceRequestStatus(String name, int code) {
		this.name = name;
		this.code = code;
	}

}
