package io.odilon.api;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Serve favicon.ico from the classpath to avoid NoResourceFoundException when the browser requests it.
 */
@RestController
public class FaviconController {

    @GetMapping(value = "/favicon.ico")
    public ResponseEntity<Resource> favicon() throws IOException {
        Resource resource = new ClassPathResource("favicon.ico");
        if (!resource.exists()) {
            return ResponseEntity.notFound().build();
        }

        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("image/x-icon"))
                .cacheControl(CacheControl.maxAge(365, TimeUnit.DAYS).cachePublic())
                .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(resource.contentLength()))
                .body(resource);
    }
}
