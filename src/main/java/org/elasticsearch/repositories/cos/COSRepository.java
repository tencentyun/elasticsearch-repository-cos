package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
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
        String app_id = COSRepository.getSetting(COSClientSettings.APP_ID, metadata);
        // qcloud-sdk-v5 app_id directly joined with bucket name
        this.bucket = bucket+"-"+app_id;
        this.client = cos.getClient();

        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : basePath.split(File.separator)) {
                path = path.add(elem);
            }
            this.basePath = path;
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
        return new COSBlobStore(settings,client,bucket);
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
