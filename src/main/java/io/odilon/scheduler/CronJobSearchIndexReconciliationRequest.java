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

import com.fasterxml.jackson.annotation.JsonTypeName;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.search.SearchIndexReconciler;

/**
 * <p>
 * {@link CronJobRequest} that periodically reconciles the Lucene search index
 * with the Odilon storage (the source of truth): re-indexes drifted or missing
 * objects and removes orphan documents.
 * </p>
 * <p>
 * This job repairs the drift caused by entries dropped when the search queue
 * was full, by requests dropped after exhausting retries, or by index loss.
 * </p>
 * 
 * @see SearchIndexReconciler
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Component
@Scope("prototype")
@JsonTypeName("searchIndexReconciliation")
public class CronJobSearchIndexReconciliationRequest extends CronJobRequest {

	static private Logger logger = Logger.getLogger(CronJobSearchIndexReconciliationRequest.class.getName());

	private static final long serialVersionUID = 1L;

	public CronJobSearchIndexReconciliationRequest() {
		super();
	}

	public CronJobSearchIndexReconciliationRequest(String exp) {
		super(exp);
	}

	@Override
	public void execute() {

		try {
			setStatus(ServiceRequestStatus.RUNNING);
			SearchIndexReconciler reconciler = getApplicationContext().getBean(SearchIndexReconciler.class);
			reconciler.reconcile(false);

		} catch (Exception e) {
			logger.error(e, SharedConstant.NOT_THROWN);

		} finally {
			setStatus(ServiceRequestStatus.COMPLETED);
		}
	}

	@Override
	public boolean isSuccess() {
		return true;
	}

	@Override
	public String getUUID() {
		return "s" + getId().toString();
	}
}
