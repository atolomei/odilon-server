package io.odilon.vfs;

import java.io.File;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.OffsetDateTime;

import org.springframework.context.ConfigurableApplicationContext;

import com.google.common.io.Files;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ODModelObject;
import io.odilon.model.OdilonServerInfo;
import io.odilon.model.ServerConstant;
import io.odilon.service.util.ByteToString;
import io.odilon.vfs.model.IODriver;
import io.odilon.vfs.model.VirtualFileSystemService;

public class EncryptionInitializer extends ODModelObject {
		
	static private Logger logger = Logger.getLogger(EncryptionInitializer.class.getName());
	static private Logger startuplogger = Logger.getLogger("StartupLogger");

	
	private VirtualFileSystemService vfs;
	
	public EncryptionInitializer(VirtualFileSystemService vfs) {
		this.vfs=vfs;
	}

	public VirtualFileSystemService getVFS() {
		return this.vfs;
	}
	
	/**
	 * 
	 * 
	 */
	public void execute() {
		
		 	startuplogger.info("Initializing Encryption Service");
		 	startuplogger.info("");
		 	
			IODriver driver = getVFS().createVFSIODriver();
			OdilonServerInfo info = driver.getServerInfo();
				
			if (info==null)
				info=getVFS().getServerSettings().getDefaultOdilonServerInfo();

			if (info.isEncryptionIntialized()) {

				startuplogger.info("Encryption Service has already been initialized on " + info.getEncryptionIntializedDate().toString());
				startuplogger.info("The server will shutdown now.");
				try {
					
					Thread.sleep(2500);
					
				} catch (InterruptedException e) {
				}
				((ConfigurableApplicationContext) this.getVFS().getApplicationContext()).close();
				System.exit(1);
			}
			
			SecureRandom secureRandom = new SecureRandom();
			 
			byte 	[] encKey 		= new byte[VirtualFileSystemService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
			byte 	[] masterKey	= new byte[VirtualFileSystemService.AES_KEY_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
			byte	[] salt 		= new byte[VirtualFileSystemService.AES_KEY_SALT_SIZE_BITS / VirtualFileSystemService.BITS_PER_BYTE];
			byte 	[] hmac;
			 
			secureRandom.nextBytes(encKey);
			secureRandom.nextBytes(masterKey);
			secureRandom.nextBytes(salt);
			
			try {
				
				hmac = getVFS().HMAC(encKey, encKey);
				
			} catch (InvalidKeyException | NoSuchAlgorithmException e) {
				logger.error(e);
				throw new InternalCriticalException(e);
			}

			getVFS().getMasterKeyEncryptorService().setKeyToEncryptMasterKey(encKey);
					 
			driver.saveServerMasterKey(masterKey, hmac, salt);
			info.setEncryptionIntialized(true);
			info.setEncryptionIntializedDate(OffsetDateTime.now());
			
			driver.setServerInfo(info);
			
			startuplogger.info("ENCRYPTION KEY");
			startuplogger.info("--------------");
			startuplogger.info("encryption.key = " + ByteToString.byteToHexString(encKey));
			startuplogger.info("The encrytion key must be added to the 'odilon.properties' file in variable 'encryption.key' as printed above.");
			startuplogger.info("");
			startuplogger.info("MASTER KEY");
			startuplogger.info("----------");										
			startuplogger.info("Master Key -> " + ByteToString.byteToHexString(masterKey));
			startuplogger.info("");
			startuplogger.info("The master key is used internally and it is secret, it is NOT required in 'odilon.properties' anywhere else."); 
			startuplogger.info("However it may be required to restore the system in case some critical system files are");
			startuplogger.info("accidental or intentionally deleted in the future.");
			startuplogger.info("Therefore it is recommended that you store it securely.");
			startuplogger.info("");
			startuplogger.info("process completed.");
			
			// startuplogger.debug("HMAC -> " + ByteToString.byteToHexString(hmac));
			
			/** try to copy kbee.enc -> /config  */

			try {
				File srcFile = driver.getDrivesEnabled().get(0).getSysFile(VirtualFileSystemService.ENCRYPTION_KEY_FILE);
				File destFile = new File(System.getProperty("user.dir") + File.separator + "config" + File.separator + srcFile.getName());
				Files.copy(srcFile, destFile);
				startuplogger.info("");
				startuplogger.info("Odilon made a backup of the encrypted key to -> " + System.getProperty("user.dir") + File.separator + "config" + File.separator + srcFile.getName());
				startuplogger.info("");
			} catch (Exception e) {
				logger.error(e, "Backup encrypted key to -> " + System.getProperty("user.dir") + File.separator + "config");
			}
		
			startuplogger.info("The server will shutdown now.");
			
			try {

				Thread.sleep(5000);
				
			} catch (InterruptedException e) {
			}
			((ConfigurableApplicationContext) getVFS().getApplicationContext()).close();
			System.exit(0);
	}
	
	/**
	 * 
	 */
	public void notInitializedError() {
		startuplogger.info("");
		startuplogger.info("The server is configured to use encryption (ie. 'encryption.enabled=true' in file 'odilon.properties')");
		startuplogger.info("but the encryption service has not been initialized yet.");
		startuplogger.info("");
		startuplogger.info("You must either:"); 
		startuplogger.info("a. Disable encryption in 'odilon.properties' by changing the variable to 'encryption.enabled=false'");
		startuplogger.info("");
		startuplogger.info("b. Initialize the encryption service by executing '"+getEnableEncryptionScriptName()+"'");
		startuplogger.info("");
		startuplogger.info("If you execute '"+getEnableEncryptionScriptName()+"' Odilon will generate and print two AES keys");
		startuplogger.info("");
		startuplogger.info("ENCRYPTION KEY");
		startuplogger.info("--------------");
		startuplogger.info("The encrytion key must be added to the 'odilon.properties' file in variable 'encryption.key'");
		startuplogger.info("For example (this key is not real, just an example): encryption.key = b9c3b04002acbbf1f40288d7308f609c");
		startuplogger.info("");
		startuplogger.info("MASTER KEY");
		startuplogger.info("----------");
		startuplogger.info("The master key is used internally and it is secret, it is not required in 'odilon.properties' or anywhere else.");
		startuplogger.info("However it may be required to restore the system in case some critical system files are");
		startuplogger.info("accidental or intentionally deleted in the future.");
		startuplogger.info("Therefore it is recommended that you store it securely.");
		startuplogger.info("");
		startuplogger.info(ServerConstant.SEPARATOR);
		try {
			Thread.sleep(3000);
			
		} catch (InterruptedException e) {
		}
		((ConfigurableApplicationContext) getVFS().getApplicationContext()).close();
		System.exit(1);

	}

	private String getEnableEncryptionScriptName() {
		return isLinux() ? ServerConstant.ENABLE_ENCRYPTION_SCRIPT_LINUX : ServerConstant.ENABLE_ENCRYPTION_SCRIPT_WINDOWS;
	}
	
	private boolean isLinux() {
		if  (System.getenv("OS")!=null && System.getenv("OS").toLowerCase().contains("windows")) 
			return false;
		return true;
	}
	
}
