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
	

	/**
	 * 
	 * @param bucketName
	 * @param objectName
	 * @param version
	 * @return
	 
	public List<File> getDataFiles(String bucketName, String objectName, Optional<Integer> version) {
		
		List<File> list = new ArrayList<File>();
		
		if (version.isEmpty()) {
			Path start = new File(getBucketObjectDataDirPath(bucketName)).toPath();
			Stream<Path> stream = null;
			try {
				stream = Files.walk(start, 1).
						skip(1).
						filter(file -> file.getFileName()!=null);
			} catch (IOException e) {
				logger.error(e);
				throw new InternalCriticalException(e);
			}
			
			try {
				Iterator<Path> it = stream.iterator();
				while (it.hasNext()) {
					Path item=it.next();
					list.add(item.toFile());
					logger.debug(item.getFileName());
				}
			} finally {
					if (stream!=null)
						stream.close();	
			}
		}
		else {
			
			Path start = new File(getBucketObjectDataDirPath(bucketName)+File.separator+"version").toPath();
			
			
		}
		
		
		return list;
			}
	*/
	


}