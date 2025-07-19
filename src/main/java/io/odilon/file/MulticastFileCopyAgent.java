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
package io.odilon.file;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.util.DateTimeUtil;

/**
 * NOT USED
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class MulticastFileCopyAgent extends FileCopyAgent {

    static private Logger logger = Logger.getLogger(MulticastFileCopyAgent.class.getName());

    @JsonIgnore
    private ExecutorService executor;

    private OffsetDateTime start;
    private OffsetDateTime end;

    @JsonIgnore
    List<File> destination;
    
    @JsonIgnore
    File source;
    
    @JsonIgnore
    InputStream sourceStream;

    @Override
    public long durationMillisecs() {
        if (start == null || end == null)
            return -1;
        return DateTimeUtil.dateTimeDifference(start, end, ChronoUnit.MILLIS);
    }

    @Override
    public boolean execute() {

        try {

            this.start = OffsetDateTime.now();

            int size = destination.size();

            /** Thread pool */
            this.executor = Executors.newFixedThreadPool(size);

            byte[] buf = new byte[ServerConstant.BUFFER_SIZE];

            /**
             * copy 1 buffer 16k
             * 
             * paso el buffer a N threads espero que todos terminen loop
             * 
             * 
             */
            int bytesRead;

            List<OutputStream> out = new ArrayList<OutputStream>();

            destination.forEach(file -> {
                try {
                    out.add(new BufferedOutputStream(new FileOutputStream(file)));
                } catch (FileNotFoundException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            });

            List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);

            while ((bytesRead = sourceStream.read(buf, 0, buf.length)) >= 0) {

                for (int index = 0; index < size; index++) {
                    final int val = index;

                    tasks.add(() -> {
                        try {
                            out.get(val).write(buf);
                        } catch (FileNotFoundException e) {
                            throw new InternalCriticalException(e, "f: " + destination.get(val).getName());
                        } catch (IOException e) {
                            throw new InternalCriticalException(e, "f: " + destination.get(val).getName());
                        } finally {
                        }
                        return true;
                    });
                }

                // out.write(buf, 0, bytesRead);
            }

            return true;

        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            return false;

        } finally {
            this.end = OffsetDateTime.now();
            logger.info("Duration: " + DateTimeUtil.timeElapsed(this.start, this.end));
        }
    }

}
