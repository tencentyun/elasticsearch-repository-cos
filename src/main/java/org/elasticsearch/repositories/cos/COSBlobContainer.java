package org.elasticsearch.repositories.cos;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.qcloud.cos.internal.CosServiceRequest;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;

import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.*;

/**
 * A plugin to add a repository type 'cos' -- The Object Storage service in QCloud.
 */
public class COSBlobContainer extends AbstractBlobContainer {

    protected final COSBlobStore blobStore;
    protected final String keyPath;

    COSBlobContainer(BlobPath path, COSBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            SocketAccess.doPrivileged(() ->
                    blobStore.client().getObjectMetadata(blobStore.bucket(), buildKey(blobName)));
            return true;
        } catch (CosClientException e) {
            return false;
        } catch (Exception e) {
            throw new BlobStoreException("failed to check if blob exists", e);
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        try {
            COSObject object = SocketAccess.doPrivileged(() ->
                    blobStore.client().getObject(blobStore.bucket(), buildKey(blobName)));
            return object.getObjectContent();
        } catch (CosClientException e) {
            if (e instanceof CosServiceException) {
                if (404 == ((CosServiceException) e).getStatusCode()) {
                    throw new NoSuchFileException("Blob object [" + blobName + "] not found: " + e.getMessage());
                }
            }
            throw e;
        }
    }

    /**
     * This implementation ignores the failIfAlreadyExists flag as the COS API has no way to enforce this due to its weak consistency model.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        if (blobSize <= COSService.MAX_SINGLE_FILE_SIZE.getBytes()) {
            doSingleUpload(blobName, inputStream, blobSize);
        } else {
            doMultipartUpload(blobName, inputStream, blobSize);
        }
    }

    void doSingleUpload(String blobName, InputStream inputStream, long blobSize) throws IOException {
        if (blobSize > COSService.MAX_SINGLE_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than max single file size");
        }
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(blobSize);
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(blobStore.bucket(), buildKey(blobName), inputStream, meta);
        setRequestHeader(putObjectRequest);
        try {
            PutObjectResult putObjectResult = SocketAccess.doPrivileged(() ->
                    blobStore.client().putObject(putObjectRequest));
            putObjectResult.getETag();
        } catch (CosServiceException e) {
            throw new IOException("Exception when write blob [" + blobName + "]", e);
        } catch (CosClientException e) {
            throw new IOException("Exception when write blob [" + blobName + "]", e);
        }
    }

    void doMultipartUpload(String blobName, InputStream inputStream, long blobSize) throws IOException {
        long partSize = COSService.MAX_SINGLE_FILE_SIZE.getBytes();
        if (blobSize <= COSService.MAX_SINGLE_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload multipart request size [" + blobSize + "] can't be smaller than max single file size");
        }
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (nbParts - 1) * partSize + lastPartSize : "blobSize does not match multipart sizes";

        final SetOnce<String> uploadId = new SetOnce<>();
        final String bucketName = blobStore.bucket();
        boolean success = false;

        try {
            final InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, buildKey(blobName));
            setRequestHeader(request);
            InitiateMultipartUploadResult initResult = SocketAccess.doPrivileged(() ->
                    blobStore.client().initiateMultipartUpload(request));
            uploadId.set(initResult.getUploadId());
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload [" + blobName + "]");
            }
            final List<PartETag> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                UploadPartRequest uploadPartRequest = new UploadPartRequest();
                uploadPartRequest.setBucketName(blobStore.bucket());
                uploadPartRequest.setKey(buildKey(blobName));
                uploadPartRequest.setUploadId(uploadId.get());
                uploadPartRequest.setInputStream(inputStream);
                uploadPartRequest.setPartNumber(i);
                setRequestHeader(uploadPartRequest);

                if (i < nbParts) {
                    uploadPartRequest.setPartSize(partSize);
                    uploadPartRequest.setLastPart(false);
                } else {
                    uploadPartRequest.setPartSize(lastPartSize);
                    uploadPartRequest.setLastPart(true);
                }
                bytesCount += uploadPartRequest.getPartSize();

                final UploadPartResult uploadResponse = SocketAccess.doPrivileged(() ->
                        blobStore.client().uploadPart(uploadPartRequest));
                parts.add(uploadResponse.getPartETag());
            }

            if (bytesCount != blobSize) {
                throw new IOException("Failed to execute multipart upload for [" + blobName + "], expected " + blobSize
                        + "bytes sent but got " + bytesCount);
            }

            CompleteMultipartUploadRequest completeMultipartUploadRequest = new CompleteMultipartUploadRequest(blobStore.bucket(), buildKey(blobName), uploadId.get(), parts);
            setRequestHeader(completeMultipartUploadRequest);
            SocketAccess.doPrivileged(() ->
                    blobStore.client().completeMultipartUpload(completeMultipartUploadRequest));
            success = true;

        } catch (CosClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if (success == false && Strings.hasLength(uploadId.get())) {
                final AbortMultipartUploadRequest aboutRequest = new AbortMultipartUploadRequest(blobStore.bucket(), buildKey(blobName), uploadId.get());
                setRequestHeader(aboutRequest);
                SocketAccess.doPrivilegedVoid(() ->
                        blobStore.client().abortMultipartUpload(aboutRequest));
            }
        }
    }

    @Override
    public void deleteBlob(String blobName) throws IOException {
        try {
            if (!blobExists(blobName)) {
                throw new IOException("Blob [" + blobName + "] does not exist");
            }
        } catch (BlobStoreException e) {
            throw new IOException("Exception when check blob exists [" + blobName + "] " + e);
        }
        deleteBlobIgnoringIfNotExists(blobName);
    }

    @Override
    public void deleteBlobIgnoringIfNotExists(String blobName) throws IOException {
        // There is no way to know if an non-versioned object existed before the deletion
        try {
            SocketAccess.doPrivilegedVoid(() ->
                    blobStore.client().deleteObject(blobStore.bucket(), buildKey(blobName)));
        } catch (CosClientException e) {
            throw new IOException("Exception when deleting blob [" + blobName + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        try {
            return SocketAccess.doPrivileged((PrivilegedAction<Map<String, BlobMetaData>>) () -> {
                MapBuilder<String, BlobMetaData> blobsBuilder = MapBuilder.newMapBuilder();
                ObjectListing prevListing = null;
                while (true) {
                    ObjectListing list;
                    if (prevListing != null) {
                        list = blobStore.client().listNextBatchOfObjects(prevListing);
                    } else {
                        if (blobNamePrefix != null) {
                            list = blobStore.client().listObjects(blobStore.bucket(), buildKey(blobNamePrefix));
                        } else {
                            list = blobStore.client().listObjects(blobStore.bucket(), keyPath);
                        }
                    }
                    for (COSObjectSummary summary : list.getObjectSummaries()) {
                        String name = summary.getKey().substring(keyPath.length());
                        blobsBuilder.put(name, new PlainBlobMetaData(name, summary.getSize()));
                    }
                    if (list.isTruncated()) {
                        prevListing = list;
                    } else {
                        break;
                    }
                }
                return blobsBuilder.immutableMap();
            });
        } catch (final CosClientException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    protected String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Returns the number parts of size of {@code partSize} needed to reach {@code totalSize},
     * along with the size of the last (or unique) part.
     *
     * @param totalSize the total size
     * @param partSize  the part size
     * @return a {@link Tuple} containing the number of parts to fill {@code totalSize} and
     * the size of the last part
     */
    static Tuple<Long, Long> numberOfMultiparts(final long totalSize, final long partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greater than zero");
        }

        if (totalSize == 0L || totalSize <= partSize) {
            return Tuple.tuple(1L, totalSize);
        }

        final long parts = totalSize / partSize;
        final long remaining = totalSize % partSize;

        if (remaining == 0) {
            return Tuple.tuple(parts, partSize);
        } else {
            return Tuple.tuple(parts + 1, remaining);
        }
    }

    private void setRequestHeader(CosServiceRequest request) {
        if (request == null) return;
        request.putCustomRequestHeader("User-Agent", "Elasticsearch");
    }
}
