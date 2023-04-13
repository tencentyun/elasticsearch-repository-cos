/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.cos;

import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;

/**
 * Created by Ethan-Zhang on 30/03/2018.
 */
public class COSRepositoryPlugin extends Plugin implements RepositoryPlugin {

    protected COSService createStorageService(RepositoryMetadata metaData) {
        return new COSService(metaData);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env,
                                                           final NamedXContentRegistry namedXContentRegistry,
                                                           final ClusterService clusterService,
                                                           final BigArrays bigArrays,
                                                           final RecoverySettings recoverySettings) {
        return Collections.singletonMap(COSRepository.TYPE,
                (metadata) -> new COSRepository(metadata, namedXContentRegistry,
                        createStorageService(metadata), clusterService, bigArrays, recoverySettings));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(COSClientSettings.REGION, COSClientSettings.ACCESS_KEY_ID, COSClientSettings.ACCESS_KEY_SECRET,
                COSClientSettings.APP_ID, COSClientSettings.BUCKET,
                COSClientSettings.BASE_PATH, COSClientSettings.COMPRESS, COSClientSettings.CHUNK_SIZE, COSClientSettings.END_POINT);
    }
}