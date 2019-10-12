package org.elasticsearch.repositories.cos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

public class COSRepository extends BlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(COSRepository.class);
    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(logger);
    public static final String TYPE = "cos";
    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;
    private final COSService service;
    private final String bucket;

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Defaults to false.
     */
    static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);


    COSRepository(RepositoryMetaData metadata,
                  NamedXContentRegistry namedXContentRegistry,
                  COSService cos,
                  ThreadPool threadpool) {
        super(metadata, COMPRESS_SETTING.get(metadata.settings()), namedXContentRegistry, threadpool);
        this.service = cos;
        String bucket = getSetting(COSClientSettings.BUCKET, metadata);
        String basePath = getSetting(COSClientSettings.BASE_PATH, metadata);
        String app_id = COSRepository.getSetting(COSClientSettings.APP_ID, metadata);
        // qcloud-sdk-v5 app_id directly joined with bucket name
        // TODO: 考虑不要在让用户传appid的参数，因为现在bucket name直接就是带着appid了，考虑两个做兼容，并写deprecation log
        this.bucket = bucket+"-"+app_id;

        if (Strings.hasLength(basePath)) {
            if (basePath.startsWith("/")) {
                basePath = basePath.substring(1);
                deprecationLogger.deprecated("cos repository base_path trimming the leading `/`, and leading `/` whill not be supported for the cos repository in future releases");
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
        return new COSBlobStore(this.service.getClient(), this.bucket);
    }

    @Override
    public BlobPath basePath() {
        return basePath;
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
