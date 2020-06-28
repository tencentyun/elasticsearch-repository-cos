package org.elasticsearch.repositories.cos;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.qcloud.cos.exception.MultiObjectDeleteException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.*;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.Tuple;

import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.*;

/**
 * A plugin to add a repository type 'cos' -- The Object Storage service in QCloud.
 */
public class COSBlobContainer extends AbstractBlobContainer {

    private static final int MAX_BULK_DELETES = 1000;
    protected final COSBlobStore blobStore;
    protected final String keyPath;

    COSBlobContainer(BlobPath path, COSBlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
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
     * 可以忽略failIfAlreadyExists，因为cos会自动覆盖重名的object
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        if (blobSize <= COSService.MAX_SINGLE_FILE_SIZE.getBytes()) {
            doSingleUpload(blobName, inputStream, blobSize);
        } else {
            doMultipartUpload(blobName, inputStream, blobSize);
        }
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    void doSingleUpload(String blobName, InputStream inputStream, long blobSize) throws IOException {
        if (blobSize > COSService.MAX_SINGLE_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than max single file size");
        }
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(blobSize);
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(blobStore.bucket(), buildKey(blobName), inputStream, meta);
        try {
            PutObjectResult putObjectResult = SocketAccess.doPrivileged(() ->
                    blobStore.client().putObject(putObjectRequest));
        } catch (CosServiceException e) {
            throw new IOException("Exception when write blob " + blobName, e);
        } catch (CosClientException e) {
            throw new IOException("Exception when write blob " + blobName, e);
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
            InitiateMultipartUploadResult initResult = SocketAccess.doPrivileged(() ->
                    blobStore.client().initiateMultipartUpload(request));
            uploadId.set(initResult.getUploadId());
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload " + blobName);
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
            SocketAccess.doPrivileged(() ->
                    blobStore.client().completeMultipartUpload(completeMultipartUploadRequest));
            success = true;

        } catch (CosClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if (success == false && Strings.hasLength(uploadId.get())) {
                final AbortMultipartUploadRequest aboutRequest = new AbortMultipartUploadRequest(blobStore.bucket(), buildKey(blobName), uploadId.get());
                SocketAccess.doPrivilegedVoid(() ->
                        blobStore.client().abortMultipartUpload(aboutRequest));
            }
        }
    }

    @Override
    public DeleteResult delete() throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        try {
            ObjectListing prevListing = null;
            while(true) {
                ObjectListing list;
                if (prevListing != null) {
                    final ObjectListing finalPrevListing = prevListing;
                    list = SocketAccess.doPrivileged(() -> blobStore.client().listNextBatchOfObjects(finalPrevListing));
                } else {
                    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
                    listObjectsRequest.setBucketName(blobStore.bucket());
                    listObjectsRequest.setPrefix(keyPath);
                    list = SocketAccess.doPrivileged(() -> blobStore.client().listObjects(listObjectsRequest));
                }
                final List<String> blobsToDelete = new ArrayList<>();
                list.getObjectSummaries().forEach(cosObjectSummary -> {
                    deletedBlobs.incrementAndGet();
                    deletedBytes.addAndGet(cosObjectSummary.getSize());
                    blobsToDelete.add(cosObjectSummary.getKey());
                });
                if (list.isTruncated()) {
                    doDeleteBlobs(blobsToDelete, false);
                    prevListing = list;
                } else {
                    final List<String> lastBlobToDelete = new ArrayList<>(blobsToDelete);
                    lastBlobToDelete.add(keyPath);
                    doDeleteBlobs(lastBlobToDelete, false);
                    break;
                }
            }
        } catch (CosClientException e) {
            throw new IOException("Exception when deleting blob container [\" + keyPath + \"]" + e);
        }
        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        doDeleteBlobs(blobNames, true);
    }

    private void doDeleteBlobs(List<String> blobNames, boolean relative) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        final Set<String> outstanding;
        if (relative) {
            outstanding = blobNames.stream().map(this::buildKey).collect(Collectors.toSet());
        } else {
            outstanding = new HashSet<>(blobNames);
        }
        try {
            final List<DeleteObjectsRequest> deleteRequests = new ArrayList<>();
            final List<String> partition = new ArrayList<>();
            for (String key : outstanding) {
                partition.add(key);
                if (partition.size() == MAX_BULK_DELETES) {
                    deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
                    partition.clear();
                }
            }
            if (partition.isEmpty() == false) {
                deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
            }
            SocketAccess.doPrivilegedVoid( () -> {
                CosClientException aex = null;
                for (DeleteObjectsRequest deleteRequest : deleteRequests) {
                    List<String> keyInRequest = deleteRequest.getKeys().stream().map(DeleteObjectsRequest.KeyVersion::getKey).collect(Collectors.toList());
                    try {
                        blobStore.client().deleteObjects(deleteRequest);
                        outstanding.removeAll(keyInRequest);
                    } catch (MultiObjectDeleteException e) {
                        outstanding.removeAll(keyInRequest);
                        outstanding.addAll(
                                e.getErrors().stream().map(MultiObjectDeleteException.DeleteError::getKey).collect(Collectors.toList())
                        );
                        aex = ExceptionsHelper.useOrSuppress(aex, e);
                    } catch (CosClientException e) {
                        aex = ExceptionsHelper.useOrSuppress(aex, e);
                    }
                }
                if (aex != null) {
                    throw aex;
                }
            });
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs [" + outstanding + "]", e);
        }
        assert outstanding.isEmpty();
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs) {
        return new DeleteObjectsRequest(bucket).withKeys(blobs.toArray(Strings.EMPTY_ARRAY)).withQuiet(true);
    }


    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        try {
            return executeListing(generateListObjectsRequest(blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix)))
                        .stream()
                        .flatMap(listing -> listing.getObjectSummaries().stream())
                        .map(summary -> new PlainBlobMetaData(summary.getKey().substring(keyPath.length()), summary.getSize()))
                        .collect(Collectors.toMap(PlainBlobMetaData::name, Function.identity()));
        } catch (CosClientException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }

    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        try {
            Map<String, BlobContainer> a = executeListing(generateListObjectsRequest(keyPath)).stream()
                    .flatMap(listing -> listing.getCommonPrefixes().stream())
                    .map(prefix -> prefix.substring(keyPath.length()))
                    .filter(name -> name.isEmpty() == false)
                    .map(name -> name.substring(0, name.length() - 1))
                    .collect(Collectors.toMap(Function.identity(), name -> blobStore.blobContainer(path().add(name))));

            return executeListing(generateListObjectsRequest(keyPath)).stream()
                    .flatMap(listing -> listing.getCommonPrefixes().stream())
                    .map(prefix -> prefix.substring(keyPath.length()))
                    .filter(name -> name.isEmpty() == false)
                    .map(name -> name.substring(0, name.length() - 1))
                    .collect(Collectors.toMap(Function.identity(), name -> blobStore.blobContainer(path().add(name))));
        } catch (CosClientException e) {
            throw new IOException("Exception when listing children of [" + path().buildAsString() + ']', e);
        }
    }

    private List<ObjectListing> executeListing(ListObjectsRequest listObjectsRequest) {
        final List<ObjectListing> results = new ArrayList<>();
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                final ObjectListing finalPrevListing = prevListing;
                list = SocketAccess.doPrivileged(() -> blobStore.client().listNextBatchOfObjects(finalPrevListing));
            } else {
                list = SocketAccess.doPrivileged(() -> blobStore.client().listObjects(listObjectsRequest));
            }
            results.add(list);
            if (list.isTruncated()) {
                prevListing = list;
            } else {
                break;
            }
        }
        return results;
    }

    private ListObjectsRequest generateListObjectsRequest(String keyPath) {
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
        listObjectsRequest.withBucketName(blobStore.bucket()).withPrefix(keyPath).withDelimiter("/");
        return listObjectsRequest;
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
}
