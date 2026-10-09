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
package io.odilon.web.page;

import java.util.Optional;

import org.apache.wicket.AttributeModifier;
import org.apache.wicket.markup.head.CssHeaderItem;
import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.html.WebMarkupContainer;
import org.apache.wicket.markup.html.WebPage;
import org.apache.wicket.markup.html.panel.Panel;
import org.apache.wicket.model.IModel;
import org.apache.wicket.model.Model;
import org.apache.wicket.model.StringResourceModel;
import org.apache.wicket.request.mapper.parameter.PageParameters;
import org.apache.wicket.request.resource.CssResourceReference;
import org.apache.wicket.request.resource.ResourceReference;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.monitor.SystemInfoService;
import io.odilon.monitor.SystemMonitorService;
import io.odilon.search.SearchService;
import io.odilon.security.TokenService;
import io.odilon.service.ObjectStorageService;
import io.odilon.service.ServerSettings;
import io.odilon.web.ServiceLocator;
import io.odilon.web.panel.GlobalTopPanel;
import io.wktui.error.ErrorPanel;
import wktui.bootstrap.Bootstrap;

/**
 * <p>
 * Superclass of all Odilon Server UI pages (HomePage, SearchPage, InfoPage).
 * Renders the Bootstrap / odilon.css resources and the {@link GlobalTopPanel}.
 * Subclasses provide their content via Wicket markup inheritance
 * (&lt;wicket:child/&gt;).
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public abstract class BasePage extends WebPage {

	private static final long serialVersionUID = 1L;

	static private Logger logger = Logger.getLogger(BasePage.class.getName());

	private static final ResourceReference BOOTSTRAP_CSS = Bootstrap.getCssResourceReference();
	private static final ResourceReference BOOTSTRAP_JS = Bootstrap.getJavaScriptResourceReference();
	private static final ResourceReference CSS = new CssResourceReference(BasePage.class, "odilon.css");

	protected BasePage(PageParameters parameters) {
		super(parameters);
	}

	@Override
	public void onInitialize() {
		super.onInitialize();

		WebMarkupContainer viewport = new WebMarkupContainer("viewport");
		viewport.add(new AttributeModifier("name", "viewport"));
		viewport.add(new AttributeModifier("content", "width=device-width, initial-scale=1, shrink-to-fit=no"));
		add(viewport);

		WebMarkupContainer robots = new WebMarkupContainer("robots");
		robots.add(new AttributeModifier("name", "robots"));
		robots.add(new AttributeModifier("content", "NOINDEX, NOFOLLOW"));
		add(robots);

		try {
			add(new GlobalTopPanel("top-panel", Model.of(getUsername().orElse(""))));
		} catch (Exception e) {
			logger.error(e, "GlobalTopPanel could not be created", SharedConstant.NOT_THROWN);
			addOrReplace(new ErrorPanel("top-panel", e));
		}
	}

	@Override
	public void renderHead(IHeaderResponse response) {
		super.renderHead(response);

		response.render(JavaScriptHeaderItem.forReference(getApplication().getJavaScriptLibrarySettings().getJQueryReference()));
		response.render(JavaScriptHeaderItem.forReference(getApplication().getJavaScriptLibrarySettings().getWicketAjaxReference()));

		response.render(CssHeaderItem.forReference(BOOTSTRAP_CSS));
		response.render(JavaScriptHeaderItem.forReference(BOOTSTRAP_JS));
		response.render(CssHeaderItem.forReference(CSS));
	}

	/** name of the authenticated Spring Security user (normally "odilon") */
	public Optional<String> getUsername() {
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (auth == null || !auth.isAuthenticated())
			return Optional.empty();
		return Optional.of(auth.getName());
	}

	protected StringResourceModel getLabel(String key) {
		return new StringResourceModel(key, this);
	}

	protected IModel<String> getLabel(String key, String... parameter) {
		StringResourceModel model = new StringResourceModel(key, this, null);
		model.setParameters((Object[]) parameter);
		return model;
	}

	public SearchService getSearchService() {
		return ServiceLocator.getInstance().getBean(SearchService.class);
	}

	public ObjectStorageService getObjectStorageService() {
		return ServiceLocator.getInstance().getBean(ObjectStorageService.class);
	}

	public SystemInfoService getSystemInfoService() {
		return ServiceLocator.getInstance().getBean(SystemInfoService.class);
	}

	public SystemMonitorService getSystemMonitorService() {
		return ServiceLocator.getInstance().getBean(SystemMonitorService.class);
	}

	public TokenService getTokenService() {
		return ServiceLocator.getInstance().getBean(TokenService.class);
	}

	public ServerSettings getServerSettings() {
		return ServiceLocator.getInstance().getBean(ServerSettings.class);
	}

	/**
	 * Relative presigned URL to open the object inline in the browser (served by
	 * {@code /presigned/object}, which sets {@code Content-Disposition: inline}).
	 */
	public String presignedUrl(String bucketName, String objectName) {
		try {
			io.odilon.security.AuthToken token = new io.odilon.security.AuthToken(bucketName, objectName);
			return "/presigned/object?token=" + java.net.URLEncoder.encode(getTokenService().encrypt(token), java.nio.charset.StandardCharsets.UTF_8);
		} catch (Exception e) {
			logger.error(e);
			return "#";
		}
	}
}