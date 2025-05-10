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
import java.io.OutputStream;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.errors.InternalCriticalException;
import io.odilon.log.Logger;
import io.odilon.model.SharedConstant;
import io.odilon.util.Check;
import io.odilon.util.DateTimeUtil;

/**
 * 
 * <p>
 * Used by {@link RAIDSixEncoder}
 * </p>
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class ParallelFileCoypAgent extends FileCopyAgent {

    static private Logger logger = Logger.getLogger(ParallelFileCoypAgent.class.getName());

    @JsonIgnore
    private final byte[][] source;

    @JsonIgnore
    private final List<File> destination;

    @JsonIgnore
    private final Boolean[] requiresCopy;

    @JsonIgnore
    private ExecutorService executor;

    @JsonIgnore
    private OffsetDateTime start;

    @JsonIgnore
    private OffsetDateTime end;

    public ParallelFileCoypAgent(byte[][] source, List<File> destination, Boolean[] requiresCopy) {

        Check.requireNonNull(source);
        Check.requireNonNull(destination);
        Check.requireTrue(requiresCopy.length == source.length, "Error not true -> requiresCopy.length == source.length");
        Check.requireTrue(source.length >= destination.size(), "Error not true -> source.length >= destination.size()");
        if (logger.isDebugEnabled()) {
            int toCopy = 0;
            for (int n = 0; n < requiresCopy.length; n++) {
                if (requiresCopy[n].booleanValue())
                    toCopy++;
            }
            Check.requireTrue(toCopy == destination.size(), "Error toCopy must be equal to destination.size()");
        }

        this.source = source;
        this.destination = destination;
        this.requiresCopy = requiresCopy;
    }

    @Override
    public long durationMillisecs() {
        if (getStart() == null || getEnd() == null)
            return -1;
        return DateTimeUtil.dateTimeDifference(getStart(), getEnd(), ChronoUnit.MILLIS);
    }

    public ExecutorService getExecutor() {
        return executor;
    }

    public void setExecutor(ExecutorService executor) {
        this.executor = executor;
    }

    @Override
    public boolean execute() {

        try {
            setStart(OffsetDateTime.now());

            int size = getDestination().size();

            int total = this.source.length;

            if (getExecutor() == null)
                setExecutor(Executors.newFixedThreadPool(size));

            List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>(size);

            int toCopy = 0;

            for (int index = 0; index < total; index++) {

                if (requiresCopy[index].booleanValue()) {

                    final int f_val = index;
                    final int f_toCopy = toCopy++;

                    tasks.add(() -> {
                        try {

                            File outputFile = getDestination().get(f_toCopy);
                            try (OutputStream out = new BufferedOutputStream(new FileOutputStream(outputFile))) {
                                out.write(this.source[f_val]);
                            } catch (FileNotFoundException e) {
                                throw new InternalCriticalException(e, "f: " + outputFile.getName());
                            } catch (IOException e) {
                                throw new InternalCriticalException(e, "f: " + outputFile.getName());
                            }
                            return Boolean.valueOf(true);

                        } catch (Exception e) {
                            logger.error(e, SharedConstant.NOT_THROWN);
                            return Boolean.valueOf(false);
                        } finally {

                        }
                    });
                }
            }
            /** process buffer in parallel */
            try {
                List<Future<Boolean>> future = this.getExecutor().invokeAll(tasks, 15, TimeUnit.MINUTES);
                Iterator<Future<Boolean>> it = future.iterator();
                while (it.hasNext()) {
                    if (!it.next().get())
                        return false;
                }

            } catch (InterruptedException e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }

            return true;

        } catch (Exception e) {
            logger.error(e, SharedConstant.NOT_THROWN);
            return false;

        } finally {
            setEnd(OffsetDateTime.now());
        }
    }

    public OffsetDateTime getStart() {
        return start;
    }

    public void setStart(OffsetDateTime start) {
        this.start = start;
    }

    public OffsetDateTime getEnd() {
        return end;
    }

    public void setEnd(OffsetDateTime end) {
        this.end = end;
    }

    public byte[][] getSource() {
        return source;
    }

    public List<File> getDestination() {
        return destination;
    }

}
