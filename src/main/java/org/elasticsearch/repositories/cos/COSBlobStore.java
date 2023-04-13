/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.cos;

import com.qcloud.cos.COSClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;

public class COSBlobStore implements BlobStore {
    
    private static final Logger logger = LogManager.getLogger(COSBlobStore.class);
    
    private final COSClient client;
    private final String bucket;

    private final BigArrays bigArrays;
    private final ByteSizeValue bufferSize;

    COSBlobStore(COSClient client, String bucket, ByteSizeValue bufferSize, BigArrays bigArrays) {
        this.client = client;
        this.bucket = bucket;
        this.bigArrays = bigArrays;
        this.bufferSize = bufferSize;
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
    
    public BigArrays bigArrays() {
        return bigArrays;
    }
    
    public long bufferSizeInBytes() {
        return bufferSize.getBytes();
    }
}
