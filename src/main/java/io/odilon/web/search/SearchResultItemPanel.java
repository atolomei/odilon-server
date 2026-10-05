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
package io.odilon.web.search;

import java.time.format.DateTimeFormatter;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;

import io.odilon.search.SearchResult;
import io.odilon.web.page.BasePage;
import wktui.base.ModelPanel;

/**
 * <p>
 * Row element of the search {@link ResultsPanel}: object name (prefixed by the
 * bucket when searching all buckets) that opens the file inline in a new tab
 * (presigned URL), and subtitle with the file name and last modified date.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class SearchResultItemPanel extends ModelPanel<SearchResult> {

	private static final long serialVersionUID = 1L;

	private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z");

	private final boolean allBuckets;

	public SearchResultItemPanel(String id, IModel<SearchResult> model, boolean allBuckets) {
		super(id, model);
		this.allBuckets = allBuckets;
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		SearchResult r = getModel().getObject();

		//String title = allBuckets ? (r.bucketName + " / " + r.objectName) : r.objectName;

		String title =  r.objectName;

		
		ExternalLink link = new ExternalLink("link", getPresignedUrl(r));
		link.add(new Label("title", title));
		add(link);

		add(new Label("subtitle", Model.of(  r.bucketName +" - " + (r.fileName != null ? r.fileName : "") + (r.lastModified != null ? (" — " + FMT.format(r.lastModified)) : ""))));
	}

	private String getPresignedUrl(SearchResult r) {
		if (getPage() instanceof BasePage)
			return ((BasePage) getPage()).presignedUrl(r.bucketName, r.objectName);
		return "#";
	}
}
