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
package io.odilon.vfs.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

import io.odilon.util.Check;

public enum DriveStatus {

	NOTSYNC 	("notsync", 1), 
	ENABLED 	("enabled", 2),
	ARCHIVED 	("archived", 3),
	DELETED 	("deleted", 4);
	
	static List<DriveStatus> ops;
	
	private String name;
	private int code;

	private DriveStatus(String name, int code) {
		this.name = name;
		this.code = code;
	}

	public String getDescription() {
		return getDescription(Locale.getDefault());
	}
	
	public String getDescription(Locale locale) {
		ResourceBundle res = ResourceBundle.getBundle(DriveStatus.this.getClass().getName(), locale);
		return res.getString(this.getName());
	}
	
	public String toJSON() {
		StringBuilder str = new StringBuilder();
		str.append("\"name\": \"" + name + "\"");
		str.append(", \"code\": " + String.valueOf(code) );
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
	
	public static DriveStatus fromId(String id) {
		
		Check.requireNonNullStringArgument(id, "id is null or empty");
			
		try {
			int value = Integer.valueOf(id).intValue();
			return get(value);
					
		} catch (IllegalArgumentException e) {
			throw (e);
		}	
		catch (Exception e) {
			throw new IllegalArgumentException("id can not be converted int Integer -> " + id );
		}
	}
	public static List<DriveStatus> getValues() {
			
			if (ops!=null)
				return ops;
			
			ops = new ArrayList<DriveStatus>();
			
			ops.add( NOTSYNC );
			ops.add( ENABLED ); 
			ops.add( ARCHIVED );
			ops.add( DELETED );
			
			return ops;
	}
		
	public static DriveStatus get(int code) {
			if (code==ENABLED.getCode()) return ENABLED;
			if (code==ARCHIVED.getCode()) return ARCHIVED;
			if (code==DELETED.getCode()) return DELETED;
			throw new IllegalArgumentException ("unsuported code -> " + String.valueOf(code));
	}

}
