package org.elasticsearch.repositories.cos;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class COSRepository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(COSRepository.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());
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
    
    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");
    static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");
    
    /**
     * Artificial delay to introduce after a snapshot finalization or delete has finished so long as the repository is still using the
     * backwards compatible snapshot format from before
     * {@link org.elasticsearch.snapshots.SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION} ({@link org.elasticsearch.Version#V_7_6_0}).
     * This delay is necessary so that the eventually consistent nature of AWS S3 does not randomly result in repository corruption when
     * doing repository operations in rapid succession on a repository in the old metadata format.
     * This setting should not be adjusted in production when working with an AWS S3 backed repository. Doing so risks the repository
     * becoming silently corrupted. To get rid of this waiting period, either create a new S3 repository or remove all snapshots older than
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
                COMPRESS_SETTING.get(metadata.settings()), 
                namedXContentRegistry, 
                clusterService,
                bigArrays,
                recoverySettings,
                buildLocation(metadata));
        this.service = cos;
        String bucket = COSClientSettings.BUCKET.get(metadata.settings());
        if (bucket == null || !Strings.hasLength(bucket)) {
            throw new RepositoryException(metadata.name(), "No bucket defined for cos repository");
        }
        String basePath = COSClientSettings.BASE_PATH.get(metadata.settings());
        String app_id = COSClientSettings.APP_ID.get(metadata.settings());
        // qcloud-sdk-v5 app_id directly joined with bucket name
        if (Strings.hasLength(app_id)) {
            this.bucket = bucket + "-" + app_id;
            deprecationLogger.deprecate("cos_repository_secret_settings","cos repository bucket already contain app_id, and app_id will not be supported for the cos repository in future releases");
        } else {
            this.bucket = bucket;
        }

        if (basePath.startsWith("/")) {
            basePath = basePath.substring(1);
            deprecationLogger.deprecate("cos_repository_secret_settings","cos repository base_path trimming the leading `/`, and leading `/` will not be supported for the cos repository in future releases");
        }

        if (Strings.hasLength(basePath)) {
            this.basePath = new BlobPath().add(basePath);
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        this.compress = COSClientSettings.COMPRESS.get(metadata.settings());
        this.chunkSize = COSClientSettings.CHUNK_SIZE.get(metadata.settings());
        coolDown = COOLDOWN_PERIOD.get(metadata.settings());

        logger.trace("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket,
                basePath, chunkSize, compress);
    }
    
    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return org.elasticsearch.common.collect.Map.of("base_path", BASE_PATH_SETTING.get(metadata.settings()),
                "bucket", BUCKET_SETTING.get(metadata.settings()));
    }
    
    /**
     * Holds a reference to delayed repository operation {@link Scheduler.Cancellable} so it can be cancelled should the repository be
     * closed concurrently.
     */
    private final AtomicReference<Scheduler.Cancellable> finalizationFuture = new AtomicReference<>();
    
    @Override
    public void finalizeSnapshot(ShardGenerations shardGenerations, long repositoryStateId, Metadata clusterMetadata,
                                 SnapshotInfo snapshotInfo, Version repositoryMetaVersion,
                                 Function<ClusterState, ClusterState> stateTransformer,
                                 ActionListener<RepositoryData> listener) {
        if (SnapshotsService.useShardGenerations(repositoryMetaVersion) == false) {
            listener = delayedListener(listener);
        }
        super.finalizeSnapshot(shardGenerations, repositoryStateId, clusterMetadata, snapshotInfo, repositoryMetaVersion,
                stateTransformer, listener);
    }
    
    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<RepositoryData> listener) {
        if (SnapshotsService.useShardGenerations(repositoryMetaVersion) == false) {
            listener = delayedListener(listener);
        }
        super.deleteSnapshots(snapshotIds, repositoryStateId, repositoryMetaVersion, listener);
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
    
    @Override
    protected void doClose() {
        final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
        if (cancellable != null) {
            logger.debug("Repository [{}] closed during cool-down period", metadata.name());
            cancellable.cancel();
        }
        super.doClose();
    }
}
