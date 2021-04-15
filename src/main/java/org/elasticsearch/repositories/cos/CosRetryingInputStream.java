package org.elasticsearch.repositories.cos;

import com.qcloud.cos.exception.CosClientException;
import com.qcloud.cos.exception.CosServiceException;
import com.qcloud.cos.model.COSObject;
import com.qcloud.cos.model.GetObjectRequest;
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
    
    private InputStream currentStream;
    private int attempt = 1;
    private List<IOException> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
    private long currentOffset;
    private boolean closed;
    
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
        this.start = start;
        this.end = end;
        currentStream = openStream();
    }
    
    private InputStream openStream() throws IOException {
        try {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(blobStore.bucket(), blobKey);
            if (currentOffset > 0 || start > 0 || end < Long.MAX_VALUE - 1) {
                assert start + currentOffset <= end :
                        "requesting beyond end, start = " + start + " offset=" + currentOffset + " end=" + end;
                getObjectRequest.setRange(Math.addExact(start, currentOffset), end);
            }
            final COSObject s3Object = SocketAccess.doPrivileged(() -> blobStore.client().getObject(getObjectRequest));
            return s3Object.getObjectContent();
        } catch (final CosClientException e) {
            if (e instanceof CosServiceException) {
                if (404 == ((CosServiceException) e).getStatusCode()) {
                    throw new NoSuchFileException("Blob object [" + blobKey + "] not found: " + e.getMessage());
                }
            }
            throw e;
        }
    }
    
    @Override
    public int read() throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int result = currentStream.read();
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
        try {
            Streams.consumeFully(currentStream);
        } catch (Exception e2) {
            logger.trace("Failed to fully consume stream on close", e);
        }
        IOUtils.closeWhileHandlingException(currentStream);
        currentStream = openStream();
    }
    
    @Override
    public void close() throws IOException {
        try {
            Streams.consumeFully(currentStream);
        } catch (Exception e) {
            logger.trace("Failed to fully consume stream on close", e);
        }
        currentStream.close();
        closed = true;
    }
    
    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }
    
    @Override
    public void reset() {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }
    
}
