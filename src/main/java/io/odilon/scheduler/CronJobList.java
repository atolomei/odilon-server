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
package io.odilon.scheduler;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei) 
 */
public class CronJobList implements SortedSet<CronJobRequest> {
			
	static private Logger logger = LogManager.getLogger(CronJobList.class.getName());

	@JsonIgnore
	static private ObjectMapper mapper = new ObjectMapper();

	static  {
		mapper.registerModule(new ParameterNamesModule());
		mapper.registerModule(new JavaTimeModule());
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@JsonIgnore
	private TreeSet<CronJobRequest> jobs = new TreeSet<CronJobRequest>(new CronJobComparator());
	
	private class CronJobComparator implements Comparator<CronJobRequest> {
		
        @Override
        public int compare(CronJobRequest a, CronJobRequest b) {
            try {
                if (a.getTime() == null && b.getTime() == null) return 0;
                if (a.getTime() == null) return -1;
                if (b.getTime() == null) return 1;
                return a.getTime().isBefore(b.getTime()) ? -1 : 1;
                
            } catch (Exception e) {
                logger.error(e);
                return 0;
            }
        }
    }

	public synchronized CronJobRequest pollFirst() {

		CronJobRequest job = getJobs().pollFirst();
		CronJobRequest nextjob = job.clone();
        
        nextjob.setId(job.getId());
        nextjob.setTime(job.getNextTime());
        getJobs().add(nextjob);
        return job;
	}
	
	@Override
	public int size() {
		return getJobs().size();
	}

	@Override
	public boolean isEmpty() {
		return getJobs().isEmpty();
	}

	@Override
	public boolean contains(Object o) {
		return getJobs().contains(o);
	}

	@Override
	public Iterator<CronJobRequest> iterator() {
		return getJobs().iterator();
	}

	@Override
	public Object[] toArray() {
		return getJobs().toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return getJobs().toArray(a);
	}
	
	@Override
	public boolean add(CronJobRequest cre) {
		return getJobs().add(cre);
	}

	@Override
	public boolean remove(Object o) {
		return getJobs().remove(o);
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		return getJobs().containsAll(c);
	}

	@Override
	public boolean addAll(Collection<? extends CronJobRequest> c) {
		c.forEach( i -> getJobs().add(i));
		return true;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		return getJobs().retainAll(c);
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		c.forEach(i -> getJobs().remove(i));
		return true;
	}

	@Override
	public void clear() {
		getJobs().clear();
	}

	@Override
	public Comparator<? super CronJobRequest> comparator() {
		return new  CronJobComparator();
	}

	@Override
	public SortedSet<CronJobRequest> subSet(CronJobRequest fromElement, CronJobRequest toElement) {
		return getJobs().subSet(fromElement, toElement);
	}

	@Override
	public SortedSet<CronJobRequest> headSet(CronJobRequest toElement) {
		return getJobs().headSet(toElement);
	}

	@Override
	public SortedSet<CronJobRequest> tailSet(CronJobRequest fromElement) {
		return getJobs().tailSet(fromElement);
	}

	@Override
	public CronJobRequest first() {
		return jobs.first();
	}

	@Override
	public CronJobRequest last() {
		return jobs.last();
	}

	protected TreeSet<CronJobRequest> getJobs() {
        return jobs;
    }
}



//String jsonString = mapper.writeValueAsString(cre);
//{
//Map<String, String> map = new HashMap<>();
//map.put("class", cre.getClass().getName());
//map.put("json", jsonString);
//ObjectMapper mapper = new ObjectMapper();
//String jsonResult = mapper.writeValueAsString(map);
//Files.writeString(Paths.get(path), jsonResult);
//}

