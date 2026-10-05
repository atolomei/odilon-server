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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.LoadableDetachableModel;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.search.SearchResult;
import io.odilon.service.ObjectStorageService;
import io.odilon.web.ServiceLocator;
import io.odilon.web.page.BasePage;
import wktui.base.InvisiblePanel;
import wktui.base.ModelPanel;

/**
 * <p>
 * Panel displayed when a search result item is expanded: object title, file
 * name with a link to open it (presigned URL), the {@link ObjectMetadata}
 * table, and (if the object has previous versions) the
 * {@link ObjectVersionsPanel}.
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ObjectExpandedPanel extends ModelPanel<SearchResult> {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(ObjectExpandedPanel.class.getName());

	private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public ObjectExpandedPanel(String id, IModel<SearchResult> model) {
		super(id, model);
		setOutputMarkupId(true);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		SearchResult r = getModel().getObject();

		/** object title */
		//add(new Label("title", r.bucketName + " / " + r.objectName));

		/** file name with a link to open it */
		//ExternalLink link = new ExternalLink("link", getPresignedUrl(r));
		//link.add(new Label("filename", r.fileName != null ? r.fileName : r.objectName));
		//add(link);

		/** metadata */
		add(new ListView<Map.Entry<String, String>>("metadata", new LoadableDetachableModel<List<Map.Entry<String, String>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<Map.Entry<String, String>> load() {
				return new ArrayList<Map.Entry<String, String>>(getMetadataMap(getModel().getObject()).entrySet());
			}
		}) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void populateItem(ListItem<Map.Entry<String, String>> row) {
				row.add(new Label("key", row.getModelObject().getKey()));
				row.add(new Label("value", row.getModelObject().getValue()));
			}
		});

		/** versions (only if the object has previous versions) */
		if (hasVersions(r))
			add(new ObjectVersionsPanel("versions", getModel()));
		else
			add(new InvisiblePanel("versions"));
	}

	private boolean hasVersions(SearchResult r) {
		try {
			return getObjectStorageService().hasVersions(r.bucketName, r.objectName);
		} catch (Exception e) {
			logger.error(e);
			return false;
		}
	}

	private String getPresignedUrl(SearchResult r) {
		if (getPage() instanceof BasePage)
			return ((BasePage) getPage()).presignedUrl(r.bucketName, r.objectName);
		return "#";
	}

	private Map<String, String> getMetadataMap(SearchResult r) {

		Map<String, String> map = new LinkedHashMap<String, String>();

		try {
			ObjectMetadata meta = getObjectStorageService().getObjectMetadata(r.bucketName, r.objectName);

			if (meta == null)
				return map;

			map.put("bucketName", meta.bucketName);
			map.put("objectName", meta.objectName);
			map.put("fileName", meta.fileName);
			map.put("contentType", meta.contentType);
			map.put("length", String.format("%,d bytes", meta.length));
			map.put("length-src", String.format("%,d bytes", meta.sourceLength));
			
			map.put("encrypt", (meta.isEncrypt()? "true" : "false")	);
			
			map.put("version", String.valueOf(meta.version));
			map.put("creationDate", meta.creationDate != null ? FMT.format(meta.creationDate) : "-");
			map.put("lastModified", meta.lastModified != null ? FMT.format(meta.lastModified) : "-");
			map.put("etag", meta.etag);
			map.put("status", meta.status != null ? meta.status.getName() : "-");
			map.put("raid", meta.raid);
			
			if (meta.customTags != null && !meta.customTags.isEmpty()) {
				StringBuilder sb = new StringBuilder();
				meta.customTags.forEach(v -> sb.append( (sb.length()>0 ? ", ":"") + sb.append(v) ));
				map.put("customtags", sb.toString());
			}
			

			 

			

		} catch (Exception e) {
			logger.error(e);
			map.put("error", e.getClass().getSimpleName() + (e.getMessage() != null ? (" - " + e.getMessage()) : ""));
		}

		return map;
	}

	protected ObjectStorageService getObjectStorageService() {
		return ServiceLocator.getInstance().getBean(ObjectStorageService.class);
	}
}
