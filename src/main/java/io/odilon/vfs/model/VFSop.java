package io.odilon.vfs.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ResourceBundle;

public enum VFSop {
	
	CREATE_BUCKET 						("create_bucket", 0, "b"),
	UPDATE_BUCKET 						("update_bucket", 1, "b"), 
	DELETE_BUCKET 						("delete_bucket", 2, "b"),
	
	CREATE_OBJECT 						("create_object", 10, "o"),
	UPDATE_OBJECT 						("update_object", 21, "o"),
	UPDATE_OBJECT_METADATA 				("update_object_metadata", 22, "o"),
	DELETE_OBJECT 						("delete_object", 23, "o"),
	DELETE_OBJECT_PREVIOUS_VERSIONS 	("delete_object_previous_versions", 24, "o"),
	RESTORE_OBJECT_PREVIOUS_VERSION 	("restore_object_previous_versions", 25, "o"),
	
	CREATE_SERVER_METADATA 				("create_server_metadata", 70, "s"),
	UPDATE_SERVER_METADATA 				("update_server_metadata", 88, "s"),
	
	CREATE_SERVER_MASTERKEY		 		("create_server_key", 90, "s");
	
	private String name;
	private int code;
	private String enttiyGroupCode;
	
	static List<VFSop> ops;
	
	public static VFSop fromId(String id) {
		
		if (id==null)
			throw new IllegalArgumentException("id is null");
			
		try {
			int value = Integer.valueOf(id).intValue();
			return get(value);
					
		} catch (IllegalArgumentException e) {
			throw (e);
		}	
		catch (Exception e) {
			throw new IllegalArgumentException("id not integer -> " + id);
		}
	}
	
	public static List<VFSop> getValues() {
		
		if (ops!=null)
			return ops;
		
		ops = new ArrayList<VFSop>();
		
		ops.add( CREATE_BUCKET );
		ops.add( UPDATE_BUCKET ); 
		ops.add( DELETE_BUCKET );

		ops.add( CREATE_OBJECT );
		ops.add( UPDATE_OBJECT ); 
		ops.add( DELETE_OBJECT );
		
		ops.add( CREATE_SERVER_METADATA );
		ops.add( CREATE_SERVER_MASTERKEY );
		
		return ops;
	}
	
	/**
	 * @param name
	 * @return
	 */
	public static VFSop get(String name) {
		
		if (name==null)
			throw new IllegalArgumentException ("name is null");
		
		String normalized = name.toUpperCase().trim();
		
		if (normalized.equals(CREATE_BUCKET.getName())) return CREATE_BUCKET;
		if (normalized.equals(UPDATE_BUCKET.getName())) return UPDATE_BUCKET;
		if (normalized.equals(DELETE_BUCKET.getName())) return DELETE_BUCKET;

		if (normalized.equals(CREATE_OBJECT.getName())) return CREATE_OBJECT;
		if (normalized.equals(UPDATE_OBJECT.getName())) return UPDATE_OBJECT;
		if (normalized.equals(DELETE_OBJECT.getName())) return DELETE_OBJECT;
		
		if (normalized.equals(UPDATE_OBJECT_METADATA.getName())) return UPDATE_OBJECT_METADATA;

		if (normalized.equals(DELETE_OBJECT_PREVIOUS_VERSIONS.getName())) return DELETE_OBJECT_PREVIOUS_VERSIONS;
		if (normalized.equals(RESTORE_OBJECT_PREVIOUS_VERSION.getName())) return RESTORE_OBJECT_PREVIOUS_VERSION;
		
		if (normalized.equals(CREATE_SERVER_METADATA.getName())) return CREATE_SERVER_METADATA;
		if (normalized.equals(UPDATE_SERVER_METADATA.getName())) return UPDATE_SERVER_METADATA;
		
		if (normalized.equals(CREATE_SERVER_METADATA.getName())) return CREATE_SERVER_METADATA;
		if (normalized.equals(UPDATE_SERVER_METADATA.getName())) return UPDATE_SERVER_METADATA;
		
		if (normalized.equals(CREATE_SERVER_MASTERKEY.getName())) return CREATE_SERVER_MASTERKEY;
		
		throw new IllegalArgumentException ("unsuported name -> " + name);
	}
	
	public static VFSop get(int code) {
		
		if (code==CREATE_BUCKET.getCode()) return CREATE_BUCKET;
		if (code==UPDATE_BUCKET.getCode()) return UPDATE_BUCKET;
		if (code==DELETE_BUCKET.getCode()) return DELETE_BUCKET;
		
		if (code==CREATE_OBJECT.getCode()) return CREATE_OBJECT;
		if (code==UPDATE_OBJECT.getCode()) return UPDATE_OBJECT;
		if (code==DELETE_OBJECT.getCode()) return DELETE_OBJECT;
		
		if (code==UPDATE_OBJECT_METADATA.getCode()) return UPDATE_OBJECT_METADATA;
		if (code==DELETE_OBJECT_PREVIOUS_VERSIONS.getCode()) return DELETE_OBJECT_PREVIOUS_VERSIONS;
		if (code==RESTORE_OBJECT_PREVIOUS_VERSION.getCode()) return RESTORE_OBJECT_PREVIOUS_VERSION;
		if (code==CREATE_SERVER_METADATA.getCode()) return CREATE_SERVER_METADATA;
		if (code==UPDATE_SERVER_METADATA.getCode()) return UPDATE_SERVER_METADATA;
		if (code==CREATE_SERVER_METADATA.getCode()) return CREATE_SERVER_METADATA;
		if (code==UPDATE_SERVER_METADATA.getCode()) return UPDATE_SERVER_METADATA;
		if (code==CREATE_SERVER_MASTERKEY.getCode()) return CREATE_SERVER_MASTERKEY;
		
		throw new IllegalArgumentException ("unsuported code -> " + String.valueOf(code));
	}

	public String getDescription() {
		return getDescription(Locale.getDefault());
	}
	
	public String getDescription(Locale locale) {
		ResourceBundle res = ResourceBundle.getBundle(VFSop.this.getClass().getName(), locale);
		return res.getString(this.getName());
	}
	
	public String toJSON() {
		StringBuilder str = new StringBuilder();
		str.append("\"name\": \"" + name + "\"");
		str.append(", \"code\": " + code );
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
	
	public boolean isObjectOperation() {
		return	(CREATE_OBJECT.equals(this) ||
				 UPDATE_OBJECT.equals(this) ||
				 DELETE_OBJECT.equals(this) || 
				 UPDATE_OBJECT_METADATA.equals(this)); 	
	}
	
	public String getEntityGroupCode() {
		return enttiyGroupCode;
	}

	private VFSop(String name, int code, String groupCode) {
		this.name = name;
		this.code = code;
		this.enttiyGroupCode=groupCode;
	}


}
