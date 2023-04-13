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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.snapshots.SnapshotDeleteListener;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class COSRepository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(COSRepository.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());
    public static final String TYPE = "cos";

    private final BlobPath basePath;
    private final boolean compress;
    private final ByteSizeValue chunkSize;
    private final COSService service;
    private final String bucket;
    private final ByteSizeValue bufferSize;

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Defaults to false.
     */
    static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);
    
    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");
    static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");
    
    /**
     * Maximum size of files that can be uploaded using a single upload request.
     */
    static final ByteSizeValue MAX_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);
    
    /**
     * Minimum size of parts that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MIN_PART_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.MB);
    
    /**
     * Maximum size of parts that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;
    
    /**
     * Maximum size of files that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);
    
    /**
     * Default is to use 100MB (Cos defaults) for heaps above 2GB and 5% of
     * the available memory for smaller heaps.
     */
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
            Math.max(
                    ByteSizeUnit.MB.toBytes(5), // minimum value
                    Math.min(
                            ByteSizeUnit.MB.toBytes(100),
                            JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)),
            ByteSizeUnit.BYTES);
    
    /**
     * Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold,
     * the COS repository will use the Tencent Cloud Multipart Upload API to split the chunk into several parts, each of buffer_size length, and
     * to upload each part in its own request. Note that setting a buffer size lower than 5mb is not allowed since it will prevents the
     * use of the Multipart API and may result in upload errors. Defaults to the minimum between 100MB and 5% of the heap size.
     */
    static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING =
            Setting.byteSizeSetting("buffer_size", DEFAULT_BUFFER_SIZE, MIN_PART_SIZE_USING_MULTIPART, MAX_PART_SIZE_USING_MULTIPART);
    
    /**
     * Artificial delay to introduce after a snapshot finalization or delete has finished so long as the repository is still using the
     * backwards compatible snapshot format from before
     * {@link org.elasticsearch.snapshots.SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION} ({@link org.elasticsearch.Version#V_7_6_0}).
     * This delay is necessary so that the eventually consistent nature of Cos does not randomly result in repository corruption when
     * doing repository operations in rapid succession on a repository in the old metadata format.
     * This setting should not be adjusted in production when working with an Cos backed repository. Doing so risks the repository
     * becoming silently corrupted. To get rid of this waiting period, either create a new Cos repository or remove all snapshots older than
     * {@link org.elasticsearch.Version#V_7_6_0} from the repository which will trigger an upgrade of the repository metadata to the new
     * format and disable the cooldown period.
     */
    static final Setting<TimeValue> COOLDOWN_PERIOD = Setting.timeSetting(
            "cooldown_period",
            new TimeValue(3, TimeUnit.MINUTES),
            new TimeValue(0, TimeUnit.MILLISECONDS),
            Setting.Property.Dynamic);
    /**
     * Time period to delay repository operations by after finalizing or deleting a snapshot.
     * See {@link #COOLDOWN_PERIOD} for details.
     */
    private final TimeValue coolDown;

    COSRepository(RepositoryMetadata metadata,
                  NamedXContentRegistry namedXContentRegistry,
                  COSService cos,
                  final ClusterService clusterService,
                  final BigArrays bigArrays,
                  final RecoverySettings recoverySettings) {
        super(metadata,
                namedXContentRegistry, 
                clusterService,
                bigArrays,
                recoverySettings,
                buildBasePath(metadata),
                buildLocation(metadata));
        this.service = cos;
        String bucket = COSClientSettings.getConfigValue(metadata.settings(), COSClientSettings.BUCKET);
        if (bucket == null || !Strings.hasLength(bucket)) {
            throw new RepositoryException(metadata.name(), "No bucket defined for cos repository");
        }
        String basePath = COSClientSettings.getConfigValue(metadata.settings(), COSClientSettings.BASE_PATH);
        String app_id = COSClientSettings.getConfigValue(metadata.settings(), COSClientSettings.APP_ID);
        // qcloud-sdk-v5 app_id directly joined with bucket name
        if (Strings.hasLength(app_id)) {
            this.bucket = bucket + "-" + app_id;
            deprecationLogger.critical(DeprecationCategory.SECURITY, "cos_repository_secret_settings",
                    "cos repository bucket already contain app_id, and app_id will not be supported for the cos repository in future releases");
        } else {
            this.bucket = bucket;
        }

        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
            deprecationLogger.critical(DeprecationCategory.SECURITY, "cos_repository_secret_settings",
                    "cos repository base_path trimming the leading `/`, and leading `/` will not be supported for the cos repository in future releases");
        }

        if (Strings.hasLength(basePath)) {
            this.basePath = BlobPath.EMPTY.add(basePath);
        } else {
            this.basePath = BlobPath.EMPTY;
        }
        this.compress = COSClientSettings.getConfigValue(metadata.settings(), COSClientSettings.COMPRESS);
        this.chunkSize = COSClientSettings.getConfigValue(metadata.settings(), COSClientSettings.CHUNK_SIZE);
        this.bufferSize = BUFFER_SIZE_SETTING.get(metadata.settings());
        
        coolDown = COOLDOWN_PERIOD.get(metadata.settings());

        logger.trace("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket,
                basePath, chunkSize, compress);
    }
    
    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of("base_path", BASE_PATH_SETTING.get(metadata.settings()),
                "bucket", BUCKET_SETTING.get(metadata.settings()));
    }
    
    /**
     * Holds a reference to delayed repository operation {@link Scheduler.Cancellable} so it can be cancelled should the repository be
     * closed concurrently.
     */
    private final AtomicReference<Scheduler.Cancellable> finalizationFuture = new AtomicReference<>();
    
    @Override
    public void finalizeSnapshot(final FinalizeSnapshotContext finalizeSnapshotContext) {
        final FinalizeSnapshotContext wrappedFinalizeContext;
        if (SnapshotsService.useShardGenerations(finalizeSnapshotContext.repositoryMetaVersion()) == false) {
            final ListenableFuture<Void> metadataDone = new ListenableFuture<>();
            wrappedFinalizeContext = new FinalizeSnapshotContext(
                    finalizeSnapshotContext.updatedShardGenerations(),
                    finalizeSnapshotContext.repositoryStateId(),
                    finalizeSnapshotContext.clusterMetadata(),
                    finalizeSnapshotContext.snapshotInfo(),
                    finalizeSnapshotContext.repositoryMetaVersion(),
                    delayedListener(ActionListener.runAfter(finalizeSnapshotContext, () -> metadataDone.onResponse(null))),
                    info -> metadataDone.addListener(new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            finalizeSnapshotContext.onDone(info);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            assert false : e; // never fails
                        }
                    })
            );
        } else {
            wrappedFinalizeContext = finalizeSnapshotContext;
        }
        super.finalizeSnapshot(wrappedFinalizeContext);
    }
    
    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                SnapshotDeleteListener listener) {
        final SnapshotDeleteListener wrappedListener;
        if (SnapshotsService.useShardGenerations(repositoryMetaVersion)) {
            wrappedListener = listener;
        } else {
            wrappedListener = new SnapshotDeleteListener() {
                @Override
                public void onDone() {
                    listener.onDone();
                }

                @Override
                public void onRepositoryDataWritten(RepositoryData repositoryData) {
                    logCooldownInfo();
                    final Scheduler.Cancellable existing = finalizationFuture.getAndSet(threadPool.schedule(() -> {
                        final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
                        assert cancellable != null;
                        listener.onRepositoryDataWritten(repositoryData);
                    }, coolDown, ThreadPool.Names.SNAPSHOT));
                    assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
                }

                @Override
                public void onFailure(Exception e) {
                    logCooldownInfo();
                    final Scheduler.Cancellable existing = finalizationFuture.getAndSet(threadPool.schedule(() -> {
                        final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
                        assert cancellable != null;
                        listener.onFailure(e);
                    }, coolDown, ThreadPool.Names.SNAPSHOT));
                    assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
                }
            };
        }
        super.deleteSnapshots(snapshotIds, repositoryStateId, repositoryMetaVersion, wrappedListener);
    }
    
    private <T> ActionListener<T> delayedListener(ActionListener<T> listener) {
        final ActionListener<T> wrappedListener = ActionListener.runBefore(listener, () -> {
            final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
            assert cancellable != null;
        });
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                logCooldownInfo();
                final Scheduler.Cancellable existing = finalizationFuture.getAndSet(
                        threadPool.schedule(ActionRunnable.wrap(wrappedListener, l -> l.onResponse(response)),
                                coolDown, ThreadPool.Names.SNAPSHOT));
                assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
            }
            
            @Override
            public void onFailure(Exception e) {
                logCooldownInfo();
                final Scheduler.Cancellable existing = finalizationFuture.getAndSet(
                        threadPool.schedule(ActionRunnable.wrap(wrappedListener, l -> l.onFailure(e)), coolDown, ThreadPool.Names.SNAPSHOT));
                assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
            }
        };
    }
    
    private void logCooldownInfo() {
        logger.info("Sleeping for [{}] after modifying repository [{}] because it contains snapshots older than version [{}]" +
                        " and therefore is using a backwards compatible metadata format that requires this cooldown period to avoid " +
                        "repository corruption. To get rid of this message and move to the new repository metadata format, either remove " +
                        "all snapshots older than version [{}] from the repository or create a new repository at an empty location.",
                coolDown, metadata.name(), SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION,
                SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION);
    }
    
    
    @Override
    protected COSBlobStore createBlobStore() {
        return new COSBlobStore(this.service.getClient(), this.bucket, this.bufferSize, this.bigArrays);
    }

    @Override
    public BlobPath basePath() {
        return basePath;
    }


    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
    
    @Override
    protected void doClose() {
        final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
        if (cancellable != null) {
            logger.debug("Repository [{}] closed during cool-down period", metadata.name());
            cancellable.cancel();
        }
        super.doClose();
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            return BlobPath.EMPTY.add(basePath);
        } else {
            return BlobPath.EMPTY;
        }
    }
}
