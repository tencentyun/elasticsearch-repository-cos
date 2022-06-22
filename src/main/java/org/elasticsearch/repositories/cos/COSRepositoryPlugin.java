package org.elasticsearch.repositories.cos;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;

/**
 * Created by Ethan-Zhang on 30/03/2018.
 */
public class COSRepositoryPlugin extends Plugin implements RepositoryPlugin, ReloadablePlugin {

    private final Logger logger = LogManager.getLogger(COSRepositoryPlugin.class);

    final COSService service;

    public COSRepositoryPlugin(Settings settings) {
        this.service = new COSService(settings);
    }

    @Override
    public Map<String, Repository.Factory> getRepositories(
        final Environment env,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final RecoverySettings recoverySettings
    ) {
        return Collections.singletonMap(
            COSRepository.TYPE,
            (metadata) -> new COSRepository(metadata, namedXContentRegistry, service, clusterService, recoverySettings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            COSClientSettings.ACCOUNT,
            COSClientSettings.REGION,
            COSClientSettings.APP_ID,
            COSClientSettings.BUCKET,
            COSClientSettings.BASE_PATH,
            COSClientSettings.COMPRESS,
            COSClientSettings.CHUNK_SIZE,
            COSClientSettings.END_POINT
        );
    }

    @Override
    public void reload(Settings settings) {
        final Map<String, COSClientSecretSettings> cosSettings = COSClientSecretSettings.load(settings);
        if (cosSettings.isEmpty()) {
            logger.warn("If you want to use an cos repository, you need to define a cos secret configuration.");
        }
        service.refreshAndClearCache(cosSettings);
    }
}
