package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.Closeable;

//TODO: 考虑是否需要继承closeable，处理连接池等问题
public class COSService {

    private COSClient client;
    public static final ByteSizeValue MAX_SINGLE_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    COSService(RepositoryMetaData metaData) {
        this.client = createClient(metaData);
    }

    private synchronized COSClient createClient(RepositoryMetaData metaData) {
        String access_key_id = COSRepository.getSetting(COSClientSettings.ACCESS_KEY_ID, metaData);
        String access_key_secret = COSRepository.getSetting(COSClientSettings.ACCESS_KEY_SECRET, metaData);
        String region = COSRepository.getSetting(COSClientSettings.REGION, metaData);
        String endPoint = COSRepository.getSetting(COSClientSettings.CosEndPoint, metaData);

        COSCredentials cred = new BasicCOSCredentials(access_key_id, access_key_secret);
        ClientConfig clientConfig = new ClientConfig(new Region(region));
        if (Strings.hasLength(endPoint)) {
            clientConfig.setEndPointSuffix(endPoint);
        }
        COSClient client = new COSClient(cred, clientConfig);

        return client;
    }

    public COSClient getClient() {
        return this.client;
    }

}
