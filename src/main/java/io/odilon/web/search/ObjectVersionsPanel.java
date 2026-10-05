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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.link.ExternalLink;
import org.apache.wicket.markup.html.link.Link;
import org.apache.wicket.markup.html.list.ListItem;
import org.apache.wicket.markup.html.list.ListView;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.LoadableDetachableModel;
import org.apache.wicket.request.handler.resource.ResourceStreamRequestHandler;
import org.apache.wicket.request.resource.ContentDisposition;
import org.apache.wicket.util.resource.AbstractResourceStreamWriter;

import io.odilon.log.Logger;
import io.odilon.model.ObjectMetadata;
import io.odilon.search.SearchResult;
import io.odilon.service.ObjectStorageService;
import io.odilon.web.ServiceLocator;
import io.odilon.web.page.BasePage;
import wktui.base.ModelPanel;

/**
 * <p>
 * Table with all the versions of an object (head version first). For each
 * version: version number, last modified date, and a link to open the file
 * (file name in bold) with the version metadata listed below the link.
 * </p>
 * <p>
 * The head version opens inline via presigned URL; previous versions are
 * streamed by the Wicket link ({@code getObjectPreviousVersionStream}).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ObjectVersionsPanel extends ModelPanel<SearchResult> {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(ObjectVersionsPanel.class.getName());

	private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

	public ObjectVersionsPanel(String id, IModel<SearchResult> model) {
		super(id, model);
		setOutputMarkupId(true);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		final int headVersion = getHeadVersion();

		add(new ListView<ObjectMetadata>("rows", new LoadableDetachableModel<List<ObjectMetadata>>() {
			private static final long serialVersionUID = 1L;

			@Override
			protected List<ObjectMetadata> load() {
				return getVersions();
			}
		}) {
			private static final long serialVersionUID = 1L;

			@Override
			protected void populateItem(ListItem<ObjectMetadata> row) {

				ObjectMetadata meta = row.getModelObject();
				boolean isHead = (meta.version == headVersion);

				/** col 1. version number */
				row.add(new Label("version", String.valueOf(meta.version) + (isHead ? " (head)" : "")));

				/** col 2. last modified date */
				row.add(new Label("lastmodified", meta.lastModified != null ? FMT.format(meta.lastModified) : "-"));

				/** col 3. link to open the file (file name in bold) + metadata */
				String fileName = (meta.fileName != null) ? meta.fileName : meta.objectName;

				ExternalLink headLink = new ExternalLink("link-head", getPresignedUrl(meta));
				headLink.add(new Label("filename", fileName));
				headLink.setVisible(isHead);
				row.add(headLink);

				Link<ObjectMetadata> versionLink = new Link<ObjectMetadata>("link-version", row.getModel()) {
					private static final long serialVersionUID = 1L;

					@Override
					public void onClick() {
						ObjectVersionsPanel.this.openPreviousVersion(getModelObject());
					}
				};
				versionLink.add(new Label("filename", fileName));
				versionLink.setVisible(!isHead);
				row.add(versionLink);

				/** metadata: one field per line */
				row.add(new ListView<String>("fields", getMetadataFields(meta)) {
					private static final long serialVersionUID = 1L;

					@Override
					protected void populateItem(ListItem<String> field) {
						field.add(new Label("field", field.getModelObject()));
					}
				});
			}
		});
	}

	/** all versions of the object: head first, then previous versions (desc) */
	private List<ObjectMetadata> getVersions() {

		List<ObjectMetadata> list = new ArrayList<ObjectMetadata>();

		SearchResult r = getModel().getObject();

		try {
			ObjectMetadata head = getObjectStorageService().getObjectMetadata(r.bucketName, r.objectName);
			if (head != null)
				list.add(head);

			List<ObjectMetadata> previous = getObjectStorageService().getObjectMetadataAllPreviousVersions(r.bucketName, r.objectName);
			if (previous != null) {
				previous.sort(Comparator.comparingInt((ObjectMetadata m) -> m.version).reversed());
				list.addAll(previous);
			}
		} catch (Exception e) {
			logger.error(e);
		}

		return list;
	}

	private int getHeadVersion() {
		try {
			ObjectMetadata head = getObjectStorageService().getObjectMetadata(getModel().getObject().bucketName, getModel().getObject().objectName);
			return (head != null) ? head.version : -1;
		} catch (Exception e) {
			logger.error(e);
			return -1;
		}
	}

	private List<String> getMetadataFields(ObjectMetadata meta) {

		List<String> fields = new ArrayList<String>();

		fields.add("contentType: " + (meta.contentType != null ? meta.contentType : "-"));
		fields.add("length: " + String.format("%,d bytes", meta.length));
		fields.add("creationDate: " + (meta.creationDate != null ? FMT.format(meta.creationDate) : "-"));
		fields.add("lastModified: " + (meta.lastModified != null ? FMT.format(meta.lastModified) : "-"));
		fields.add("etag: " + (meta.etag != null ? meta.etag : "-"));
		fields.add("status: " + (meta.status != null ? meta.status.getName() : "-"));

		return fields;
	}

	/** streams a previous version of the object to the browser (inline) */
	private void openPreviousVersion(ObjectMetadata meta) {

		final String bucketName = meta.bucketName;
		final String objectName = meta.objectName;
		final int version = meta.version;
		final String contentType = meta.contentType;
		final String fileName = (meta.fileName != null) ? meta.fileName : meta.objectName;

		AbstractResourceStreamWriter writer = new AbstractResourceStreamWriter() {
			private static final long serialVersionUID = 1L;

			@Override
			public void write(OutputStream output) throws IOException {
				try (InputStream in = getObjectStorageService().getObjectPreviousVersionStream(bucketName, objectName, version)) {
					in.transferTo(output);
				}
			}

			@Override
			public String getContentType() {
				return contentType;
			}
		};

		ResourceStreamRequestHandler handler = new ResourceStreamRequestHandler(writer, fileName);
		handler.setContentDisposition(ContentDisposition.INLINE);
		getRequestCycle().scheduleRequestHandlerAfterCurrent(handler);
	}

	private String getPresignedUrl(ObjectMetadata meta) {
		if (getPage() instanceof BasePage)
			return ((BasePage) getPage()).presignedUrl(meta.bucketName, meta.objectName);
		return "#";
	}

	protected ObjectStorageService getObjectStorageService() {
		return ServiceLocator.getInstance().getBean(ObjectStorageService.class);
	}
}
