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
package io.odilon.api;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.ExternalDocumentation;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import io.swagger.v3.oas.models.servers.Server;

import org.springdoc.core.models.GroupedOpenApi;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <p>
 * Configures the OpenAPI 3.1 specification and Swagger UI for Odilon.
 * </p>
 *
 * <h3>URLs (default port 9234)</h3>
 * <ul>
 * <li>Swagger UI  : <a href="http://localhost:9234/api-docs/ui">http://localhost:9234/api-docs/ui</a></li>
 * <li>JSON spec   : <a href="http://localhost:9234/api-docs">http://localhost:9234/api-docs</a></li>
 * <li>YAML spec   : <a href="http://localhost:9234/api-docs.yaml">http://localhost:9234/api-docs.yaml</a></li>
 * <li>Resource API: <a href="http://localhost:9234/api-docs/resource">http://localhost:9234/api-docs/resource</a></li>
 * <li>Legacy API  : <a href="http://localhost:9234/api-docs/legacy">http://localhost:9234/api-docs/legacy</a></li>
 * </ul>
 *
 * <h3>API Groups</h3>
 * <ul>
 * <li><b>Resource API</b> — paths {@code /buckets/**} — the new resource-oriented
 *     endpoints in {@link ObjectResourceController}. Recommended for all new integrations.</li>
 * <li><b>Legacy API</b>   — paths {@code /object/**}, {@code /bucket/**} — the original
 *     action-based endpoints used by the Java SDK. Stable, not deprecated.</li>
 * </ul>
 *
 * <h3>Authentication</h3>
 * <p>
 * All endpoints use HTTP Basic authentication. In Swagger UI click the
 * <em>Authorize</em> button and enter the {@code accessKey} (username) and
 * {@code secretKey} (password) configured in {@code odilon.properties}.
 * </p>
 *
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Configuration
public class OpenApiConfig {

    private static final String SECURITY_SCHEME_NAME = "basicAuth";

    /**
     * Root OpenAPI bean — defines title, version, license, contact, servers
     * and the HTTP Basic security scheme applied globally to all operations.
     */
    @Bean
    public OpenAPI odilonOpenAPI() {
        return new OpenAPI()
                .info(new Info()
                        .title("Odilon Object Storage API")
                        .version("2.1")
                        .description("""
                                Odilon is an open-source Object Storage server designed for \
                                medium-to-large binary objects (PDF, photos, audio, video). \
                                It supports software RAID (0 / 1 / 6 - Erasure Coding), AES-256 encryption, \
                                version control and master-standby replication.\n\n\
                                Two API surfaces are available:\n\
                                * **Resource API** (`/buckets/**`) — REST-style, recommended for new integrations.\n\
                                * **Legacy API** (`/object/**`, `/bucket/**`) — used by the Java SDK, fully supported.\
                                """)
                        .license(new License()
                                .name("Apache 2.0")
                                .url("https://www.apache.org/licenses/LICENSE-2.0"))
                        .contact(new Contact()
                                .name("Odilon / kbee")
                                .url("https://odilon.io")
                                .email("info@novamens.com")))

                .addServersItem(new Server()
                        .url("http://localhost:9234")
                        .description("Local development server (default port)"))
                .addServersItem(new Server()
                        .url("https://your-odilon-server:9234")
                        .description("Production server — replace with your hostname"))

                .externalDocs(new ExternalDocumentation()
                        .description("Odilon documentation")
                        .url("https://odilon.io/documentation.html"))

                // Declare HTTP Basic as the single security scheme
                .components(new Components()
                        .addSecuritySchemes(SECURITY_SCHEME_NAME,
                                new SecurityScheme()
                                        .type(SecurityScheme.Type.HTTP)
                                        .scheme("basic")
                                        .description("HTTP Basic authentication. " +
                                                "Use the accessKey as username and secretKey as password " +
                                                "(configured in odilon.properties).")))

                // Apply the scheme globally — every operation requires Basic auth
                .addSecurityItem(new SecurityRequirement().addList(SECURITY_SCHEME_NAME));
    }

    /**
     * <b>Resource API group</b> — the new REST-style surface at {@code /buckets/**}.
     * Documented with full {@code @Operation} / {@code @ApiResponse} annotations.
     * Recommended entry point for all new integrations.
     */
    @Bean
    public GroupedOpenApi resourceApi() {
        return GroupedOpenApi.builder()
                .group("resource")
                .displayName("Resource API  (/buckets)")
                .pathsToMatch("/buckets/**")
                .build();
    }

    /**
     * <b>Legacy API group</b> — the original action-based surface at
     * {@code /object/**} and {@code /bucket/**}, used by the Java SDK.
     * Endpoints are tagged but not fully annotated with {@code @Operation}.
     */
    @Bean
    public GroupedOpenApi legacyApi() {
        return GroupedOpenApi.builder()
                .group("legacy")
                .displayName("Legacy API  (/object, /bucket)")
                .pathsToMatch("/object/**", "/bucket/**")
                .build();
    }
}
