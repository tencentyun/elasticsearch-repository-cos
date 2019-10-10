package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

public class COSBlobStore implements BlobStore {
    private final COSClient client;
    private final String bucket;


    COSBlobStore(COSClient client, String bucket) {
        this.client = client;
        this.bucket = bucket;
    }

    @Override
    public String toString() {
        return SocketAccess.doPrivileged(() ->
                client.getClientConfig().getRegion() + "/" + bucket);
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new COSBlobContainer(path, this);
    }

    @Override
    public void close() {
        SocketAccess.doPrivilegedVoid(() -> client.shutdown());
    }

    public COSClient client() {
        return client;
    }

    public String bucket() {
        return bucket;
    }
}
