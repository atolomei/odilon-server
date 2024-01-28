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

package io.odilon.migration;

import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.xmlpull.v1.XmlPullParserException;

import io.minio.MinioClient;
import io.minio.ObjectStat;
import io.minio.Result;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidArgumentException;
import io.minio.errors.InvalidBucketNameException;
import io.minio.errors.InvalidEndpointException;
import io.minio.errors.InvalidPortException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.NoResponseException;
import io.minio.messages.Bucket;
import io.minio.messages.Item;
import io.odilon.client.ODClient;
import io.odilon.client.OdilonClient;
import io.odilon.client.error.ODClientException;
import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;

/**
 * <p>This program migrates a <a href="https://minio.io">Minio Server</a> 
 * to <a href="https://odilon.io">Odilon</a></p>
 * 
 * <p>The client version of the Minio SDK used here is <b>6.0.13</b> (see pom.xml). <br/>
 * The API methods may be different for newer versions of the Minio SDK</p>
 * 
 * <p>Note that some <b>objectName</b> may be valid for Minio but not for Odilon, in those cases 
 * the migration process converts the objectName into a Odilon supported objectName</p>
 * 
 * <p>Minio SDK returns <b>bucketName</b>, <b>objectName</b> and other values but it does not return the file name (like <i>"test.pdf"</i>), 
 * therefore it is not possible to know the file name from Minio SDK. 
 * We use the objectName as a file name.</p>
 * 
 * <p>Logs:
 * <ul>
 * <li><b>stats.log</b> -> summarized info</li>
 * <li><b>bucket-object.log</b> -> list of objectNames that were normalized to be valid for Odilon </li>
 * <li><b>minio-migration.log</b> -> console log</li>
 * </ul>
 * </p>
 * <p>
 * Sample JVM args: 
 * <ul>
 * <li>-Xbootclasspath/a:.\config</li>
 * <li>-Dlog4j.configurationFile=.\config\log4j2.xml</li>
 * <li>-Xms1G</li>
 * <li>-Xmx4G</li>
 * <li>-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector</li>
 * <li>-DMinioEndpoint=http://localhost:9000</li>
 * <li>-DMinioAccessKey=minio</li>
 * <li>-DMinioSecretKey=minio</li>
 * <li>-DOdilonEndpoint=http://localhost</li>
 * <li>-DOdilonAccessKey=odilon</li>
 * <li>-DOdilonSecretKey=odilon</li>
 * <li>-DOdilonPort=92345</li>
 * <li>-DforceAll=false</li>
 * </ul>
 * </p>
 */
public class MinioMigration {
			
	private static final Logger logger = Logger.getLogger(MinioMigration.class.getName());
	private static final Logger objectNameNormalizedLogger = Logger.getLogger("bucket-object");
	private static final Logger statsLogger = Logger.getLogger("stats");
								
	private MinioClient minioClient;
	private OdilonClient odilonClient;
	
	private int maxProcessingThread  = 1;
	
	/** Minio credentials */
	private String minioEndpoint = "http://localhost:9000";
	private String minioAccessKey = "G17YNO4C7RSYXPNGGKJE";
	private String minioSecretKey = "fz0VPr0Sh3DtTYWVtlkERR+tE68kVUvwfffHdvgA";
	
	/** Odilon credentials */
	private String odilonEndpoint = "http://localhost";
	private int odilonPort = 9234;
	private String odilonAccessKey = "odilon";
	private String odilonSecretKey = "odilon";

	/**
	 *  forceAll:
	 *  
	 *  false (default) -> only migrate files whose creation date is older that the migrated version 
	 *  true -> migrate all files regardless of whether they were migrated before
	 *  
	 * */
	private boolean forceAll = false;
	
	
	private OffsetDateTime start;
	private OffsetDateTime end;
	
	private Map<String, MigrationMetrics> metrics = new ConcurrentHashMap<String, MigrationMetrics>();

	private int totalMinioBuckets = 0;
	private int PAGE_SIZE = 10;
	private ExecutorService executor;
	 
	/**
	 * 
	 */
	private class MigrationMetrics {
	
		public  MigrationMetrics( String bucketName) {
			this.bucketName=bucketName;
		}

		public OffsetDateTime start;
		public OffsetDateTime end;
		 
		public String bucketName;

		public AtomicLong filesReadMinio = new AtomicLong(0);
		public AtomicLong totalBytesReadMinio = new AtomicLong(0);
		
		public AtomicLong filesAlreadyMigrated = new AtomicLong(0);
		public AtomicLong filesSavedOdilon = new AtomicLong(0);
		public AtomicLong totalBytesSavedOdilon = new AtomicLong(0);
		
		public AtomicLong errorsReadingMinio = new AtomicLong(0);
		public AtomicLong errorsSavingOdilon = new AtomicLong(0);
		
		@SuppressWarnings("unused")
		public AtomicBoolean success = new AtomicBoolean(false);
	}
	 /**
	  *
	  */
	public static void main(String[] args) {
		MinioMigration minioToOdilon = new MinioMigration(args);
		minioToOdilon.run();
	}

	/**
	 *
	 */
	public MinioMigration(String[] args) {
		logger.info("Starting -> " + getClass().getSimpleName());
		if(args!=null) {
			Arrays.asList(args).forEach( item ->
					{
						String param[] = item.trim().split("=");
						switch (param[0]) {
							case "-DMinioEndpoint"		: {minioEndpoint 	= param[1].trim(); break;}
							case "-DMinioAccessKey" 	: {minioAccessKey  	= param[1].trim(); break;}
							case "-DMinioSecretKey" 	: {minioSecretKey  	= param[1].trim(); break;}
							case "-DOdilonEndpoint" 	: {odilonEndpoint 	= param[1].trim(); break;}
							case "-DOdilonPort" 		: {odilonPort 		= Integer.valueOf(param[1].trim()).intValue(); break;}
							case "-DOdilonAccessKey" 	: {odilonAccessKey 	= param[1].trim(); break;}
							case "-DOdilonSecretKey" 	: {odilonSecretKey 	= param[1].trim(); break;}
							case "-DforceAll"		 	: {forceAll 		= param[1].trim().equals("true") ? true : false;  break;}
							default:
						}
					}
			);
		}
	}
	
	/**
	 * 
	 */
	public void run() {

		try {
			this.start = OffsetDateTime.now();
		
			/** the migration process uses as many threads as possible */
			this.maxProcessingThread = Double.valueOf(Double.valueOf(Runtime.getRuntime().availableProcessors()-1)).intValue();
			
			if(this.maxProcessingThread<1)
				this.maxProcessingThread=1;	
			
			/** PAGE SIZE is a the size of buffer read from Minio and pass to the thread pool that copies them into Odilon */
			this.PAGE_SIZE = this.maxProcessingThread * 100;
			
			logger.info("Thread pool -> " + String.valueOf(this.maxProcessingThread ));
			logger.info("Force migrate all -> " + String.valueOf( forceMigrateAll() ));
			
			boolean bMinio = connectMinio();
			if (!bMinio)
				error("Minio not available");
			
			boolean bOdilon = connectOdilon();
			if (!bOdilon)
				error("Odilon not available");
		
			/** Thread pool */
			this.executor = Executors.newFixedThreadPool(this.maxProcessingThread);
			
			try {
				List<Bucket> minioBuckets = getMinioClient().listBuckets();
				this.totalMinioBuckets = minioBuckets.size();
				logger.info("Total Minio buckets -> " + String.valueOf(this.totalMinioBuckets));
				minioBuckets.forEach(item -> migrate(item.name()));
				
			} catch (Exception e) {
				logger.error(e);
				error("Migration stopped.");
			}
			
			try {
				this.executor.shutdown();
				this.executor.awaitTermination(10, TimeUnit.MINUTES);
				
			} catch (InterruptedException e) {
			}
			
		} finally {

			this.end = OffsetDateTime.now();

			logResults(logger);
			logResults(statsLogger);
			
		}
	}

	/**
	 * <p>Migrate a Bucket and its objects. <br/> 
	 * If the Bucket does not exist in Odilon, it will first create it.
	 * forceAll
	 *  
	 * </p>
	 * @param bucketName
	 */
	private void migrate(final String bucketName) {
		
		Check.requireNonNullStringArgument(bucketName, "bucketName is null");
		
		MigrationMetrics bucketMetrics = new MigrationMetrics(bucketName); 
					
		bucketMetrics.start = OffsetDateTime.now();

		try {
			try {
				if (!getOdilonClient().existsBucket(bucketName)) {
					logger.info("Creating Odilon bucket -> " + bucketName);
					getOdilonClient().createBucket(bucketName);
				}
			} catch (ODClientException e) {
				logger.error(e);
				error(e);
			}

			logger.info("Starting bucket " + String.valueOf(metrics.size()+1) + "/" + String.valueOf(totalMinioBuckets) + ": " + bucketName   );
					
			Iterator<Result<Item>> it = null;
			
			try {
				it = getMinioClient().listObjects(bucketName).iterator();
			} catch (XmlPullParserException e) {
				logger.error(e);
				error(e);
			}
			
			OffsetDateTime showStatus = this.start;
			
			while (it.hasNext()) {
				
				/** 
				 * load a buffer of SIZE items and process in parallel 
				 * */
				int bufferSize=0;
				
				List<Callable<Object>> tasks = new ArrayList<>(PAGE_SIZE);
				
				while (it.hasNext() && bufferSize++<PAGE_SIZE) {
			
					Item item = null;

					try {

						item = it.next().get();
						
					} catch (InvalidKeyException | InvalidBucketNameException | NoSuchAlgorithmException
							| InsufficientDataException | NoResponseException | ErrorResponseException
							| InternalException | IOException | XmlPullParserException e) {
							logger.error(e);	
							error(e);
					}
					
					ObjectStat objectStat = null;
					
					try {
						/** read object metadata from Minio */
						objectStat = getMinioClient().statObject(bucketName, item.objectName());
						
					} catch (InvalidKeyException | InvalidBucketNameException | NoSuchAlgorithmException
							| InsufficientDataException | NoResponseException | ErrorResponseException
							| InternalException | InvalidResponseException | InvalidArgumentException | IOException
							| XmlPullParserException e) {
							logger.error(e);
							error(e);
					}
					
					long lapse = dateTimeDifference( showStatus, OffsetDateTime.now(), ChronoUnit.MILLIS);
					
					/** display status every 2 seconds or so */
					if (lapse>2000) {
						long filesReadMinio		= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).filesReadMinio.get()).sum();
						long filesSavedOdilon	= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).filesSavedOdilon.get()).sum();
						
						logger.info( "files read all buckets -> " + String.valueOf(filesReadMinio) + " | " + " | files migrated all buckets -> " + String.valueOf(filesSavedOdilon));
						
						showStatus = OffsetDateTime.now();
					}

					final ObjectStat obStat = objectStat;
					
					tasks.add( () -> {

						InputStream stream = null;
						
						try {
						
								/** read binary from Minio */
								try {
								
									bucketMetrics.filesReadMinio.getAndIncrement();
									bucketMetrics.totalBytesReadMinio.getAndAdd(obStat.length());
									 
									stream = getMinioClient().getObject(bucketName, obStat.name());
									
								} catch (Exception e) {
									logger.error(e, "reading from Minio -> b:" + bucketName + "| o: " + obStat.name());
									bucketMetrics.errorsReadingMinio.getAndIncrement();
									bucketMetrics.success = new AtomicBoolean(false);
								}
								
								/** save to Odilon */
		
								boolean requiresSync = true;
								
								String normalizedName = obStat.name().replace("/", "-").replace("\\", "-");
								
								try {
										/** if object exists in Odilon skip. Note that Minio uses a 
										 * Date for creationDate, which is ambiguous, we assume UTC 
										 * */
										if (getOdilonClient().existsObject(bucketName, normalizedName)) {
												ObjectMetadata meta = getOdilonClient().getObjectMetadata(bucketName, normalizedName);
												OffsetDateTime offsetDateTime = obStat.createdTime().toInstant().atOffset(ZoneOffset.UTC);
												requiresSync = meta.lastModified.isBefore(offsetDateTime);
												if (!requiresSync) 
													bucketMetrics.filesAlreadyMigrated.getAndIncrement();
										}
								} catch (Exception e) {
										logger.error(e, "reading from Minio -> b:" + bucketName + "| o: " + obStat.name());
										error(e);
								}
		
										
								if (forceMigrateAll() || requiresSync) {
										try {
											if (!normalizedName.equals(obStat.name()))
												objectNameNormalizedLogger.info("Normalized | b:" + bucketName + " o:" + obStat.name() + " -> " + " o:" + normalizedName);
											
											ObjectMetadata meta = getOdilonClient().putObjectStream(
																						bucketName, 
																						normalizedName, 
																						stream,
																						Optional.ofNullable(normalizedName), 
																						Optional.ofNullable(obStat.length())
																					);
												
											bucketMetrics.filesSavedOdilon.getAndIncrement();
											bucketMetrics.totalBytesSavedOdilon.getAndAdd(meta.length());
											bucketMetrics.success = new AtomicBoolean(true);
									
										} catch (Exception e) {
											logger.error(e, "saving to Odilon -> b:" + bucketName + "| o: " + normalizedName);
											bucketMetrics.errorsSavingOdilon.getAndIncrement();
											bucketMetrics.success = new AtomicBoolean(false);
										}
								}
								else {
										
								}
								return null;
						} finally {
							if (stream!=null)
								stream.close();
						}
					});
				}
				/** end load buffer */
				
				/** process buffer in parallel */
				try {
					executor.invokeAll(tasks, 10, TimeUnit.MINUTES);						
				} catch (InterruptedException e) {
					logger.error(e);
				}
			}
			
		} finally {
				bucketMetrics.end=OffsetDateTime.now();
				metrics.put(bucketName, bucketMetrics); 
				logBucketResults(statsLogger, bucketMetrics);
		}
	}

	private boolean forceMigrateAll() {
		return forceAll;
	}

	/**
	 * 
	 */
	private boolean connectMinio() {
		try {
			this.minioClient = new MinioClient(minioEndpoint, minioAccessKey, minioSecretKey);
			logger.info("Minio connected ok");
			return true;
			
		} catch (InvalidEndpointException | InvalidPortException e) {
			logger.error(e);
			return false;
		}
	}

	private boolean connectOdilon() {
		try {
			this.odilonClient = new ODClient(odilonEndpoint, odilonPort, odilonAccessKey, odilonSecretKey);
			
			if (this.odilonClient.ping().equals("ok")) {
				logger.info("Odilon connected ok");
				return true;
			}
			else { 
				logger.error(this.odilonClient.ping());
				return false;
			}
			
		} catch (Exception e) {
			logger.error(e);
			return false;
		}
	}
	
	private void error(Exception e) {
		error(e.getClass().getName() +( e.getMessage()!=null ? (" | " + e.getMessage()) : ""));
	}

	private void error(String string) {
		logger.error(string);
		System.exit(1);
	}
	
	private MinioClient getMinioClient() {
		return minioClient;
	}

	private OdilonClient getOdilonClient() {
		return odilonClient;
	}

	private void logBucketResults(Logger logger, MigrationMetrics metrics) {
	
		logger.info("Threads: " + String.valueOf(maxProcessingThread));
		logger.info("Bucket: " + metrics.bucketName);
		logger.info("Total files load from Minio: " + String.valueOf(metrics.filesReadMinio.get()));
		logger.info("Total size read from Minio: " + String.format("%14.4f", Double.valueOf(metrics.totalBytesReadMinio.get()).doubleValue() / SharedConstant.d_gigabyte).trim() + " GB");
		logger.info("Total files already migrated to Odilon: " + String.valueOf(metrics.filesAlreadyMigrated.get()));
		logger.info("Total files saved to Odilon: " + String.valueOf(metrics.filesSavedOdilon.get()));
		logger.info("Total size saved to Odilon: " + String.format("%14.4f", Double.valueOf(metrics.totalBytesSavedOdilon.get()).doubleValue() / SharedConstant.d_gigabyte).trim() + " GB");
		
		if (metrics.errorsReadingMinio.get()>0)
			logger.info("Error reading Minio: " + String.valueOf(metrics.errorsReadingMinio.get()));
		
		if (metrics.errorsSavingOdilon.get()>0)
			logger.info("Error saving Odilon: " + String.valueOf(metrics.errorsSavingOdilon.get()));
		
		logger.info("Duration: " + timeElapsed(metrics.start, metrics.end));
		logger.info("---------");

	}

	
	/**
	 * 
	 */
	private void logResults(Logger logger) {

		long filesReadMinio 		= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).filesReadMinio.get()).sum();
		long totalBytesReadMinio 	= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).totalBytesReadMinio.get()).sum();
		long filesAlreadyMigrated 	= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).filesAlreadyMigrated.get()).sum();
		long filesSavedOdilon 		= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).filesSavedOdilon.get()).sum();
		long totalBytesSavedOdilon 	= metrics.entrySet().stream().mapToLong( m -> (m.getValue()).totalBytesSavedOdilon.get()).sum();

		logger.info("MIGRATION RESULTS");
		logger.info("-----------------");
		logger.info("Total Buckets -> " + String.valueOf(metrics.size()));
		logger.info("Total files load from Minio -> " + String.valueOf(filesReadMinio));
		logger.info("Total size read from Minio -> " + String.format("%14.4f",Double.valueOf(totalBytesReadMinio).doubleValue() / SharedConstant.d_gigabyte).trim() + " GB");
		logger.info("Total files that were already in Odilon -> " + String.valueOf(filesAlreadyMigrated));
		logger.info("Total files migrated to Odilon -> " + String.valueOf(filesSavedOdilon));
		logger.info("Total size migrated to Odilon -> " + String.format("%14.4f", Double.valueOf(totalBytesSavedOdilon).doubleValue() / SharedConstant.d_gigabyte).trim() + " GB");
		logger.info("Total duration -> " + timeElapsed(this.start, this.end));
	}
	
	/**
	 * @param from
	 * @param to
	 * @return
	 */
	private String timeElapsed(OffsetDateTime from, OffsetDateTime to) {
		
		Check.requireNonNullArgument(from, "from is null");		
		Check.requireNonNullArgument(to, "to is null");
		
		if (from.plusSeconds(1).isAfter(to))
			return String.valueOf(dateTimeDifference(from, to, ChronoUnit.MILLIS))+" millis";
		
		long diff=dateTimeDifference(from, to, ChronoUnit.MILLIS);
		
		/** less than 5 min, display in seconds */
		if (from.plusMinutes(5).isAfter(to)) {
			double diffSecs = Double.valueOf(diff) / 1000.0;
			return String.format("%8.2f secs", diffSecs).trim();
		}
		 
		/** less than 1 hr, display in minutes */
		if (from.plusHours(1).isAfter(to)) {
			double diffMins = Double.valueOf(diff) / (1000.0*60.0);
			return String.format("%8.2f min", diffMins).trim();
		}
		
		/** more than 1 hr, display in hours */
		double diffHours = Double.valueOf(diff) / (1000.0*60.0*60.0);
		return String.format("%8.2f hr", diffHours).trim();
	}
	
	private long dateTimeDifference(Temporal d1, Temporal d2, ChronoUnit unit) {
        return unit.between(d1, d2);
    }
}

