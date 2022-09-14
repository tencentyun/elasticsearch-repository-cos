package org.elasticsearch.repositories.cos;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.AbstractThirdPartyRepositoryTestCase;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.blankOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNot.not;

public class CosRepositoryThirdPartyTests extends AbstractThirdPartyRepositoryTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(COSRepositoryPlugin.class);
    }

    protected SecureSettings credentials() {
        assertThat(System.getProperty("access_key_id"), not(blankOrNullString()));
        assertThat(System.getProperty("access_key_secret"), not(blankOrNullString()));
        assertThat(System.getProperty("bucket"), not(blankOrNullString()));

        MockSecureSettings secureSettings = new MockSecureSettings();
        //secureSettings.setString("access_key_id", System.getProperty("access_key_id"));
        //secureSettings.setString("access_key_secret", System.getProperty("access_key_secret"));
        return secureSettings;
    }

    @Override
    protected void createRepository(String repoName) {
        final Client client = client();

        //MockSecureSettings secureSettings = new MockSecureSettings();
        //secureSettings.setString("access_key_id", System.getProperty("access_key_id"));
        //secureSettings.setString("access_key_secret", System.getProperty("access_key_secret"));

        AcknowledgedResponse putReposirotyResponse =
                client.admin().cluster().preparePutRepository(repoName)
                .setType(COSRepository.TYPE)
                .setSettings(Settings.builder()
                        //.setSecureSettings(secureSettings)
                        .put("access_key_id", System.getProperty("access_key_id"))
                        .put("access_key_secret", System.getProperty("access_key_secret"))
                        .put("bucket",System.getProperty("bucket"))
                        .put("base_path",System.getProperty("base_path"))
                        .put("region",System.getProperty("region")))
                .get();

        assertThat(putReposirotyResponse.isAcknowledged(), equalTo(true));
    }
}
