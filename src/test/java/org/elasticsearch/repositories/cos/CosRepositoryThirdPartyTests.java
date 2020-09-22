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

    @Override
    protected boolean assertCorruptionVisible(BlobStoreRepository repo, Executor genericExec) throws Exception {
        // S3 is only eventually consistent for the list operations used by this assertions so we retry for 10 minutes assuming that
        // listing operations will become consistent within these 10 minutes.
        assertBusy(() -> assertTrue(super.assertCorruptionVisible(repo, genericExec)), 30L, TimeUnit.SECONDS);
        return true;
    }

    @Override
    protected void assertConsistentRepository(BlobStoreRepository repo, Executor executor) throws Exception {
        // S3 is only eventually consistent for the list operations used by this assertions so we retry for 10 minutes assuming that
        // listing operations will become consistent within these 10 minutes.
        assertBusy(() -> super.assertConsistentRepository(repo, executor), 30L, TimeUnit.SECONDS);
    }

    protected void assertBlobsByPrefix(BlobPath path, String prefix, Map<String, BlobMetadata> blobs) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertBlobsByPrefix(path, prefix, blobs), 30L, TimeUnit.SECONDS);
    }

    @Override
    protected void assertChildren(BlobPath path, Collection<String> children) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertChildren(path, children), 30L, TimeUnit.SECONDS);
    }

    @Override
    protected void assertDeleted(BlobPath path, String name) throws Exception {
        // AWS S3 is eventually consistent so we retry for 10 minutes assuming a list operation will never take longer than that
        // to become consistent.
        assertBusy(() -> super.assertDeleted(path, name), 30L, TimeUnit.SECONDS);
    }

}
