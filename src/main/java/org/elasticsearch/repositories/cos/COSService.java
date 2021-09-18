package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.ClientConfig;
import com.qcloud.cos.auth.BasicCOSCredentials;
import com.qcloud.cos.auth.COSCredentials;
import com.qcloud.cos.region.Region;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.repositories.RepositoryException;

import java.io.Closeable;
import java.io.IOException;

//TODO: 考虑是否需要继承closeable，处理连接池等问题
public class COSService implements Closeable {
    private static final Logger logger = LogManager.getLogger(COSService.class);

    private COSClient client;
    public static final ByteSizeValue MAX_SINGLE_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    COSService(RepositoryMetadata metaData) {
        this.client = createClient(metaData);
    }

    private synchronized COSClient createClient(RepositoryMetadata metaData) {
        String access_key_id = COSClientSettings.ACCESS_KEY_ID.get(metaData.settings());
        String access_key_secret = COSClientSettings.ACCESS_KEY_SECRET.get(metaData.settings());
        String region = COSClientSettings.REGION.get(metaData.settings());
        if (region == null || !Strings.hasLength(region)) {
            throw new RepositoryException(metaData.name(), "No region defined for cos repository");
        }
        String endPoint = COSClientSettings.END_POINT.get(metaData.settings());

        COSCredentials cred = new BasicCOSCredentials(access_key_id, access_key_secret);
        
        ClientConfig clientConfig = SocketAccess.doPrivileged(() -> new ClientConfig(new Region(region)));
        if (Strings.hasLength(endPoint)) {
            clientConfig.setEndPointSuffix(endPoint);
        }
        COSClient client = new COSClient(cred, clientConfig);

        return client;
    }

    public COSClient getClient() {
        return this.client;
    }

    @Override
    public void close() throws IOException {
        this.client.shutdown();
    }

}
