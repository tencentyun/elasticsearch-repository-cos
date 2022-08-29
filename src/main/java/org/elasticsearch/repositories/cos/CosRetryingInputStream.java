package org.elasticsearch.repositories.cos;

import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.internal.CosServiceRequest;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.COSObjectInputStream;
import com.qcloud.cos.model.GetObjectRequest;
import com.qcloud.cos.model.ObjectMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

public class CosRetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(CosRetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final COSBlobStore blobStore;
    private final String blobKey;
    private final long start;
    private final long end;
    private final int maxAttempts;
    private final List<IOException> failures;

    private COSObjectInputStream currentStream;
    private long currentStreamLastOffset;
    private int attempt = 1;
    private long currentOffset;
    private boolean closed;
    private boolean eof;

    CosRetryingInputStream(COSBlobStore blobStore, String blobKey) throws IOException {
        this(blobStore, blobKey, 0, Long.MAX_VALUE - 1);
    }

    // both start and end are inclusive bounds, following the definition in GetObjectRequest.setRange
    CosRetryingInputStream(COSBlobStore blobStore, String blobKey, long start, long end) throws IOException {
        if (start < 0L) {
            throw new IllegalArgumentException("start must be non-negative");
        }
        if (end < start || end == Long.MAX_VALUE) {
            throw new IllegalArgumentException("end must be >= start and not Long.MAX_VALUE");
        }
        this.blobStore = blobStore;
        this.blobKey = blobKey;
        this.maxAttempts = 11;
        this.failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
        this.start = start;
        this.end = end;
        openStream();
    }

    private void openStream() throws IOException {
        try {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(blobStore.bucket(), blobKey);
            setRequestHeader(getObjectRequest);
            if (currentOffset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                assert start + currentOffset <= end :
                        "requesting beyond end, start = " + start + " offset=" + currentOffset + " end=" + end;
                getObjectRequest.setRange(Math.addExact(start, currentOffset), end);
            }
            final COSObject s3Object = SocketAccess.doPrivileged(() -> blobStore.client().getObject(getObjectRequest));
            this.currentStreamLastOffset = Math.addExact(Math.addExact(start, currentOffset), getStreamLength(s3Object));
            this.currentStream = s3Object.getObjectContent();
        } catch (final CosClientException e) {
            if (e instanceof CosServiceException) {
                if (404 == ((CosServiceException) e).getStatusCode()) {
                    throw new NoSuchFileException("Blob object [" + blobKey + "] not found: " + e.getMessage());
                }
            }
            throw e;
        }
    }

    private long getStreamLength(final COSObject object) {
        final ObjectMetadata metadata = object.getObjectMetadata();
        try {
            // Returns the content range of the object if response contains the Content-Range header.
            return metadata.getContentLength();
        } catch (Exception e) {
            assert false : e;
            return Long.MAX_VALUE - 1L; // assume a large stream so that the underlying stream is aborted on closing, unless eof is reached
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int result = currentStream.read();
                if (result == -1) {
                    eof = true;
                    return -1;
                }
                currentOffset += 1;
                return result;
            } catch (IOException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead == -1) {
                    eof = true;
                    return -1;
                }
                currentOffset += bytesRead;
                return bytesRead;
            } catch (IOException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using CosRetryingInputStream after close";
            throw new IllegalStateException("using CosRetryingInputStream after close");
        }
    }

    private void reopenStreamOrFail(IOException e) throws IOException {
        if (attempt >= maxAttempts) {
            logger.debug(new ParameterizedMessage("failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], giving up",
                    blobStore.bucket(), blobKey, start + currentOffset, attempt, maxAttempts), e);
            throw e;
        }
        logger.debug(new ParameterizedMessage("failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], retrying",
                blobStore.bucket(), blobKey, start + currentOffset, attempt, maxAttempts), e);
        attempt += 1;
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(e);
        }
        maybeAbort(currentStream);
        IOUtils.closeWhileHandlingException(currentStream);
        openStream();
    }

    @Override
    public void close() throws IOException {
        maybeAbort(currentStream);
        try {
            currentStream.close();
        } finally {
            closed = true;
        }
    }

    private void maybeAbort(COSObjectInputStream stream) {
        if (isEof()) {
            return;
        }
        try {
            if (start + currentOffset < currentStreamLastOffset) {
                stream.abort();
            }
        } catch (Exception e) {
            logger.warn("Failed to abort stream before closing", e);
        }
    }

    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    // package-private for tests
    boolean isEof() {
        return eof || start + currentOffset == currentStreamLastOffset;
    }

    // package-private for tests
    boolean isAborted() {
        if (currentStream == null || currentStream.getHttpRequest() == null) {
            return false;
        }
        return currentStream.getHttpRequest().isAborted();
    }

    private void setRequestHeader(CosServiceRequest request) {
        if (request == null) return;
        request.putCustomRequestHeader("User-Agent", "Elasticsearch");
    }
}
