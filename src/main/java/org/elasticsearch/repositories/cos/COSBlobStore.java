package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import com.qcloud.cos.model.COSObjectSummary;
import com.qcloud.cos.model.DeleteObjectsRequest;
import com.qcloud.cos.model.ObjectListing;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;

import java.util.ArrayList;

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
    public void delete(BlobPath path) {
        SocketAccess.doPrivilegedVoid(() -> {
            ObjectListing prevListing = null;
            DeleteObjectsRequest multiObjectDeleteRequest = null;
            ArrayList<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();
            while (true) {
                ObjectListing list;
                if (prevListing != null) {
                    list = client.listNextBatchOfObjects(prevListing);
                } else {
                    list = client.listObjects(bucket, path.buildAsString());
                    multiObjectDeleteRequest = new DeleteObjectsRequest(list.getBucketName());
                }
                for (COSObjectSummary summary : list.getObjectSummaries()) {
                    keys.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
                    if (keys.size() > 500) {
                        multiObjectDeleteRequest.setKeys(keys);
                        client.deleteObjects(multiObjectDeleteRequest);
                        multiObjectDeleteRequest = new DeleteObjectsRequest(list.getBucketName());
                        keys.clear();
                    }
                }
                if (list.isTruncated()) {
                    prevListing = list;
                } else {
                    break;
                }
            }

            if (!keys.isEmpty()) {
                multiObjectDeleteRequest.setKeys(keys);
                client.deleteObjects(multiObjectDeleteRequest);
            }
        });
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
