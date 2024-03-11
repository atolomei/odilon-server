package io.odilon.vfs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;

/**
* <p>For RAID 6</p>
*/
@Component
@Scope("prototype")
public class ChunkedDrive extends ODDrive {

	
	static private Logger logger = Logger.getLogger(ODSimpleDrive.class.getName());
	
	@Autowired
	protected ChunkedDrive(String rootDir) {
		super(rootDir);
	}
	
	protected ChunkedDrive(String name, String rootDir) {
		super(rootDir);
		setName(name);
		onInitialize();
	}
	
 
	


}