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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import io.odilon.log.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.odilon.model.SharedConstant;

/**
 * 
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
public class CronJobList implements SortedSet<CronJobRequest> {

	static private Logger logger = Logger.getLogger(CronJobList.class.getName());

	private class CronJobComparator implements Comparator<CronJobRequest> {
		@Override
		public int compare(CronJobRequest a, CronJobRequest b) {
			try {
				if (a.getTime() == null && b.getTime() == null) {
					// fall through to tiebreaker
				} else if (a.getTime() == null) {
					return -1;
				} else if (b.getTime() == null) {
					return 1;
				} else {
					int timeComp = a.getTime().compareTo(b.getTime());
					if (timeComp != 0)
						return timeComp;
				}
				// Tiebreaker by ID to keep both entries in the TreeSet and maintain a
				// consistent total order — prevents silent job loss when two cron jobs
				// are scheduled at exactly the same instant.
				if (a.getId() == null && b.getId() == null)
					return 0;
				if (a.getId() == null)
					return -1;
				if (b.getId() == null)
					return 1;
				return a.getId().toString().compareTo(b.getId().toString());
			} catch (Exception e) {
				logger.error(e, SharedConstant.NOT_THROWN);
				return 0;
			}
		}
	}

	@JsonIgnore
	private TreeSet<CronJobRequest> jobs = new TreeSet<CronJobRequest>(new CronJobComparator());

	public synchronized CronJobRequest pollFirst() {

		CronJobRequest job = getJobs().pollFirst();
		if (job == null)
			return null;
		CronJobRequest nextjob = job.clone();

		nextjob.setId(job.getId());
		nextjob.setTime(job.getNextTime());
		getJobs().add(nextjob);
		return job;
	}

	@Override
	public synchronized int size() {
		return getJobs().size();
	}

	@Override
	public synchronized boolean isEmpty() {
		return getJobs().isEmpty();
	}

	@Override
	public synchronized boolean contains(Object o) {
		return getJobs().contains(o);
	}

	@Override
	public synchronized Iterator<CronJobRequest> iterator() {
		return getJobs().iterator();
	}

	@Override
	public synchronized Object[] toArray() {
		return getJobs().toArray();
	}

	@Override
	public synchronized <T> T[] toArray(T[] a) {
		return getJobs().toArray(a);
	}

	@Override
	public synchronized boolean add(CronJobRequest cre) {
		return getJobs().add(cre);
	}

	@Override
	public synchronized boolean remove(Object o) {
		return getJobs().remove(o);
	}

	@Override
	public synchronized boolean containsAll(Collection<?> c) {
		return getJobs().containsAll(c);
	}

	@Override
	public synchronized boolean addAll(Collection<? extends CronJobRequest> c) {
		c.forEach(i -> getJobs().add(i));
		return true;
	}

	@Override
	public synchronized boolean retainAll(Collection<?> c) {
		return getJobs().retainAll(c);
	}

	@Override
	public synchronized boolean removeAll(Collection<?> c) {
		c.forEach(i -> getJobs().remove(i));
		return true;
	}

	@Override
	public synchronized void clear() {
		getJobs().clear();
	}

	@Override
	public Comparator<? super CronJobRequest> comparator() {
		return new CronJobComparator();
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
	public synchronized CronJobRequest first() {
		return this.jobs.first();
	}

	@Override
	public synchronized CronJobRequest last() {
		return this.jobs.last();
	}

	protected TreeSet<CronJobRequest> getJobs() {
		return this.jobs;
	}
}
