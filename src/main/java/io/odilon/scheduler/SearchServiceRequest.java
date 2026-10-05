/*
 * Odilon Object Storage
 * (c) kbee 
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

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.SharedConstant;
import io.odilon.search.SearchService;
import io.odilon.virtualFileSystem.OdilonVirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemOperation;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * <p>
 * Asynchronous Lucene index update for a committed
 * {@link VirtualFileSystemOperation}. Created inside the atomic transaction
 * (by {@code OdilonJournalService.commit()} via
 * {@link io.odilon.search.SearchIndexService}) and executed later by the
 * {@link SearchSchedulerWorker} thread pool.
 * </p>
 * <p>
 * The request re-reads the <b>current head</b> {@link ObjectMetadata} from
 * storage at execution time — Lucene is never the source of truth, so indexing
 * whatever is the head version now is always correct, even if several
 * operations on the same object were queued.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("searchIndex")
public class SearchServiceRequest extends AbstractServiceRequest {

	static private Logger logger = Logger.getLogger(SearchServiceRequest.class.getName());

	private static final long serialVersionUID = 1L;

	@JsonProperty("operation")
	private OdilonVirtualFileSystemOperation operation;

	@JsonIgnore
	private boolean isSuccess = false;

	protected SearchServiceRequest() {
	}

	public SearchServiceRequest(VirtualFileSystemOperation operation) {
		this.operation = (OdilonVirtualFileSystemOperation) operation;
	}

	/**
	 * <p>
	 * {@link ServiceRequestExecutor} closes the Request after this method
	 * </p>
	 */
	@Override
	public void execute() {

		try {
			setStatus(ServiceRequestStatus.RUNNING);

			SearchService searchService = getApplicationContext().getBean(SearchService.class);

			if (!searchService.isEnabled()) {
				this.isSuccess = true;
				setStatus(ServiceRequestStatus.COMPLETED);
				return;
			}

			VirtualFileSystemService vfs = getApplicationContext().getBean(VirtualFileSystemService.class);

			switch (getVFSOperation().getOperationCode()) {

			case CREATE_OBJECT:
			case UPDATE_OBJECT:
			case UPDATE_OBJECT_METADATA:
			case RESTORE_OBJECT_PREVIOUS_VERSION: {
				indexHead(vfs, searchService);
				break;
			}

			case DELETE_OBJECT: {
				searchService.delete(getVFSOperation().getBucketName(), getVFSOperation().getObjectName());
				break;
			}

			case DELETE_BUCKET: {
				searchService.deleteBucket(getVFSOperation().getBucketName());
				break;
			}

			case UPDATE_BUCKET: {
				/**
				 * for UPDATE_BUCKET the journal stores the new bucket name in the objectName
				 * slot (see OdilonJournalService.updateBucket)
				 */
				searchService.renameBucket(getVFSOperation().getBucketName(), getVFSOperation().getObjectName());
				break;
			}

			default:
				break;
			}

			this.isSuccess = true;
			setStatus(ServiceRequestStatus.COMPLETED);

		} catch (Exception e) {
			this.isSuccess = false;
			setStatus(ServiceRequestStatus.ERROR);
			logger.error(e, SharedConstant.NOT_THROWN);
		}
	}

	private void indexHead(VirtualFileSystemService vfs, SearchService searchService) {

		String bucketName = getVFSOperation().getBucketName();
		String objectName = getVFSOperation().getObjectName();

		if (!vfs.existsBucket(bucketName)) {
			/** bucket removed since commit -> remove from index */
			searchService.delete(bucketName, objectName);
			return;
		}

		ServerBucket bucket = vfs.getBucketByName(bucketName);

		if (!vfs.existsObject(bucket, objectName)) {
			/** object removed since commit -> remove from index */
			searchService.delete(bucketName, objectName);
			return;
		}

		ObjectMetadata meta = vfs.getObjectMetadata(bucket, objectName);
		if (meta != null)
			searchService.index(meta);
	}

	@Override
	public void stop() {
		this.isSuccess = false;
		setStatus(ServiceRequestStatus.STOPPED);
	}

	@JsonIgnore
	@Override
	public String getUUID() {
		return getVFSOperation().getUUID();
	}

	@JsonIgnore
	public VirtualFileSystemOperation getVFSOperation() {
		return this.operation;
	}

	@JsonIgnore
	@Override
	public boolean isSuccess() {
		return this.isSuccess;
	}
}
