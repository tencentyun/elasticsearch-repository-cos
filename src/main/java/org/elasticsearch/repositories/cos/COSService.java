package org.elasticsearch.repositories.cos;

import static java.util.Collections.emptyMap;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.RepositoryException;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;

//TODO: 考虑是否需要继承closeable，处理连接池等问题
public class COSService {
    private static final Logger logger = LogManager.getLogger(COSService.class);

    public static final ByteSizeValue MAX_SINGLE_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    volatile Map<String, COSClientSecretSettings> secretSettings = emptyMap();

    public COSService(Settings settings) {
        // eagerly load client settings so that secure settings are read
        final Map<String, COSClientSecretSettings> clientsSettings = COSClientSecretSettings.load(settings);
        refreshAndClearCache(clientsSettings);
    }

    public synchronized COSClient createClient(RepositoryMetadata metaData) {
        Tuple<String, String> secret = getSecretTuple(metaData);
        String access_key_id = secret.v1();
        String access_key_secret = secret.v2();

        String region = COSClientSettings.REGION.get(metaData.settings());
        if (region == null || !Strings.hasLength(region)) {
            throw new RepositoryException(metaData.name(), "No region defined for cos repository");
        }
        String endPoint = COSClientSettings.END_POINT.get(metaData.settings());

        COSCredentials cred = new BasicCOSCredentials(access_key_id, access_key_secret);
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        if (Strings.hasLength(endPoint)) {
            clientConfig.setEndPointSuffix(endPoint);
        }
        COSClient client = new COSClient(cred, clientConfig);

        return client;
    }

    public Map<String, COSClientSecretSettings> refreshAndClearCache(Map<String, COSClientSecretSettings> clientsSettings) {
        final Map<String, COSClientSecretSettings> prevSettings = this.secretSettings;
        this.secretSettings = MapBuilder.newMapBuilder(clientsSettings).immutableMap();
        return prevSettings;
    }

    private Tuple<String, String> getSecretTuple(RepositoryMetadata metaData) {
        // meta setting first
        String access_key_id = COSClientSettings.ACCESS_KEY_ID.get(metaData.settings());
        String access_key_secret = COSClientSettings.ACCESS_KEY_SECRET.get(metaData.settings());
        if (access_key_id == null
            || !Strings.hasLength(access_key_id)
            || access_key_secret == null
            || !Strings.hasLength(access_key_secret)) {
            // secret setting
            String account = COSClientSettings.ACCOUNT.get(metaData.settings());
            final COSClientSecretSettings cosSecretSetting = this.secretSettings.get(account);
            if (cosSecretSetting == null) {
                throw new SettingsException("Unable to find cos repo secret settings with name [" + account + "]");
            }
            access_key_id = cosSecretSetting.getSecretId();
            access_key_secret = cosSecretSetting.getSecretKey();
        }
        return new Tuple<>(access_key_id, access_key_secret);
    }

}
