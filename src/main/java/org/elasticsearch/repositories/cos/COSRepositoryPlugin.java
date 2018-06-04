package org.elasticsearch.repositories.cos;

import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.elasticsearch.SpecialPermission;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;

/**
 * Created by Ethan-Zhang on 30/03/2018.
 */
public class COSRepositoryPlugin extends Plugin implements RepositoryPlugin {

    protected COSService createStorageService(Settings settings, RepositoryMetaData metaData) {
        return new COSService(settings, metaData);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(Environment env,
                                                                     NamedXContentRegistry namedXContentRegistry) {
        return Collections.singletonMap(COSRepository.TYPE,
                (metadata) -> new COSRepository(metadata, env.settings(), namedXContentRegistry,
                        createStorageService(env.settings(), metadata)));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(COSClientSettings.REGION, COSClientSettings.ACCESS_KEY_ID, COSClientSettings.ACCESS_KEY_SECRET,
                COSClientSettings.APP_ID, COSClientSettings.BUCKET,
                COSClientSettings.BASE_PATH, COSClientSettings.COMPRESS, COSClientSettings.CHUNK_SIZE);
    }
}
