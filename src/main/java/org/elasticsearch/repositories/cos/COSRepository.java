package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.File;
import java.io.IOException;

public class COSRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(COSRepository.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);

    public static final String TYPE = "cos";
    private final String bucket;
    private COSClient client;
    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;

    COSRepository(RepositoryMetaData metadata, Settings settings,
                 NamedXContentRegistry namedXContentRegistry, COSService cos) throws IOException {
        super(metadata, settings, namedXContentRegistry);
        String bucket = getSetting(COSClientSettings.BUCKET, metadata);
        String basePath = getSetting(COSClientSettings.BASE_PATH, metadata);
        String app_id = COSClientSettings.APP_ID.get(metadata.settings());
        this.client = cos.getClient();
        // qcloud-sdk-v5 app_id directly joined with bucket name
        if (Strings.hasLength(app_id)) {
            this.bucket = bucket + "-" + app_id;
            deprecationLogger.deprecated("cos repository bucket already contain app_id, and app_id will not be supported for the cos repository in future releases");
        } else {
            this.bucket = bucket;
        }

        if (Strings.hasLength(basePath)) {
            if (basePath.startsWith("/")) {
                basePath = basePath.substring(1);
                deprecationLogger.deprecated("cos repository base_path trimming the leading `/`, and leading `/` will not be supported for the cos repository in future releases");
            }
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        this.compress = getSetting(COSClientSettings.COMPRESS, metadata);
        this.chunkSize = getSetting(COSClientSettings.CHUNK_SIZE, metadata);

        logger.trace("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket,
                basePath, chunkSize, compress);

    }

    @Override
    protected COSBlobStore createBlobStore() {
        return new COSBlobStore(client,bucket);
    }

    @Override
    protected BlobPath basePath() {
        return basePath;
    }

    @Override
    protected boolean isCompress() {
        return compress;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    public static <T> T getSetting(Setting<T> setting, RepositoryMetaData metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(),
                    "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if ((value instanceof String) && (Strings.hasText((String) value)) == false) {
            throw new RepositoryException(metadata.name(),
                    "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }
}
