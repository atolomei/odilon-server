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
package io.odilon.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.odilon.client.error.ODClientException;
import io.odilon.log.Logger;
import io.odilon.model.Bucket;
import io.odilon.model.ObjectMetadata;
import io.odilon.model.ServerConstant;
import io.odilon.model.SharedConstant;
import io.odilon.model.list.DataList;
import io.odilon.model.list.Item;
import io.odilon.model.list.ResultSet;
import io.odilon.service.BaseService;
import io.odilon.virtualFileSystem.model.ServerBucket;
import io.odilon.virtualFileSystem.model.VirtualFileSystemService;

/**
 * @author atolomei@novamens.com (Alejandro Tolomei)
 */
@Service
public class ReplicationCheckService extends BaseService {

    static private Logger logger = Logger.getLogger(ReplicationCheckService.class.getName());

    @Autowired
    private final ReplicationService replicationService;

    @Autowired
    private final VirtualFileSystemService virtualFileSystemService;

    public ReplicationCheckService(ReplicationService replicationService, VirtualFileSystemService virtualFileSystemService) {
        this.replicationService = replicationService;
        this.virtualFileSystemService = virtualFileSystemService;
    }

    public void check() {
        checkBuckets();
        for (ServerBucket bucket : getVirtualFileSystemService().listAllBuckets()) {
            checkBucket(bucket);
        }
    }

    public ReplicationService getReplicationService() {
        return this.replicationService;
    }

    protected void checkBuckets() {

        List<String> bucketsLocalNotRemote = new ArrayList<String>();
        List<String> bucketsRemoteNotLocal = new ArrayList<String>();

        for (ServerBucket bucket : getVirtualFileSystemService().listAllBuckets()) {
            try {
                if (!getReplicationService().getClient().existsBucket(bucket.getName())) {
                    bucketsLocalNotRemote.add(bucket.getName());
                }
            } catch (ODClientException e) {
                logger.error(e, SharedConstant.NOT_THROWN);
            }
        }

        try {
            for (Bucket bucket : getReplicationService().getClient().listBuckets()) {
                if (!getVirtualFileSystemService().existsBucket(bucket.getName())) {
                    bucketsRemoteNotLocal.add(bucket.getName());
                }
            }

        } catch (ODClientException e) {
            logger.error(e, SharedConstant.NOT_THROWN);
        }

        bucketsLocalNotRemote.forEach(n -> logger.error(n));
        bucketsRemoteNotLocal.forEach(n -> logger.error(n));
    }

    protected void checkBucket(ServerBucket bucket) {

        List<String> localNotRemote = new ArrayList<String>();
        List<String> remoteNotLocal = new ArrayList<String>();

        List<String> errors = new ArrayList<String>();

        {
            Integer pageSize = Integer.valueOf(ServerConstant.DEFAULT_COMMANDS_PAGE_SIZE);
            Long offset = Long.valueOf(0);
            String agentId = null;

            boolean done = false;
            while (!done) {

                DataList<Item<ObjectMetadata>> data = getVirtualFileSystemService().listObjects(bucket.getName(),
                        Optional.of(offset), Optional.ofNullable(pageSize), Optional.empty(), Optional.ofNullable(agentId));
                if (agentId == null)
                    agentId = data.getAgentId();

                for (Item<ObjectMetadata> item : data.getList()) {
                    if (item.isOk()) {
                        try {
                            if (!getReplicationService().getClient().existsObject(item.getObject().bucketName,
                                    item.getObject().objectName)) {
                                localNotRemote.add(item.getObject().bucketId.toString() + " /" + item.getObject().objectName);
                            }
                        } catch (ODClientException | IOException e) {
                            errors.add(e.getClass().getName());
                        }
                    }
                }

                offset += Long.valueOf(Integer.valueOf(data.getList().size()).longValue());
                done = data.isEOD();
            }
        }

        {
            boolean done = false;

            while (!done) {

                ResultSet<Item<ObjectMetadata>> data;
                try {

                    data = getReplicationService().getClient().listObjects(bucket.getName());

                    while (data.hasNext()) {
                        Item<ObjectMetadata> item = data.next();
                        if (data.next().isOk()) {
                            try {
                                if (!getVirtualFileSystemService().existsObject(item.getObject().bucketName,
                                        item.getObject().objectName)) {
                                    remoteNotLocal.add(item.getObject().bucketName + " /" + item.getObject().objectName);
                                }
                            } catch (Exception e) {
                                errors.add(e.getClass().getName());
                            }
                        }
                    }

                } catch (ODClientException e) {
                    logger.error(e, SharedConstant.NOT_THROWN);
                }
            }
        }
        localNotRemote.forEach(n -> logger.error(n));
        remoteNotLocal.forEach(n -> logger.error(n));
    }

    private VirtualFileSystemService getVirtualFileSystemService() {
        return virtualFileSystemService;
    }
}
