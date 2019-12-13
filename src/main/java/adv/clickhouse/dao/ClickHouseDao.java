package adv.clickhouse.dao;

import adv.util.*;
import adv.clickhouse.BatchWriter;
import adv.clickhouse.ChAnnotationScanner;
import adv.clickhouse.model.DbEvent;
import org.asynchttpclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.validation.constraints.NotNull;
import java.beans.IntrospectionException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static adv.util.Check.notNull;
import static adv.util.DateUtil.now;

@SuppressWarnings("unchecked")
@Component
public class ClickHouseDao {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDao.class);

    @Autowired
    @Qualifier("clickHouseJdbcTemplate")
    JdbcTemplate clickTemplate;

    @Autowired
    @Qualifier("batchPool")
    private ScheduledExecutorService scheduler;

    @Value("${clickhouse.httpUrl}")
    private String clickhouseHttpUrl;

    @Value("${clickhouse.trigger.delay}")
    private int triggerDelay;

    @Value("${clickhouse.trigger.batchSize}")
    private int triggerBatchSize;

    @Value("${clickhouse.db}")
    private String clickhouseDb;

    @Value("${clickhouse.pkg}")
    private String clickhousePkg;

    @Value("${clickhouse.ioThreads:1}")
    private int ioThreads;

    @Value("${clickhouse.insert.success.maxAgeHours:24}")
    private int maxSuccessAgeHours;

    private AsyncHttpClient httpClient;

    // TODO: хранить BatchWriter вместо событий на запись
    private Map<Class, ConcurrentLinkedQueue<DbEvent>> writeCache = new ConcurrentHashMap<>();

    private ChAnnotationScanner annotationScanner;

    private AtomicInteger insertCount = new AtomicInteger(0);

    private AtomicBoolean networkActive = new AtomicBoolean(false);

    private Map<Class, StringBuilder> buffers = new HashMap<>();
    private Map<Class<? extends DbEvent>, BatchWriter<? extends DbEvent>> batches = new HashMap<>();

    @PostConstruct
    public void init() throws IllegalAccessException, InvocationTargetException, IntrospectionException, IOException {
        log.debug("init AsyncHttpClient");
        AsyncHttpClientConfig asyncHttpClientConfig = new DefaultAsyncHttpClientConfig.Builder()
                .setThreadFactory(ExecutorUtil.createNamedThreadFactory("clickhouse-dao-ahc-"))
                .setIoThreadsCount(ioThreads)
                .build();
        httpClient = new DefaultAsyncHttpClient(asyncHttpClientConfig);

        annotationScanner = new ChAnnotationScanner(clickhouseDb, clickhousePkg);
        log.debug("init clickhouse connection");

        try {
            FileUtil.mkDirOrFail(BatchTask.writingDir.toFile());
            FileUtil.mkDirOrFail(BatchTask.pendingDir.toFile());
            FileUtil.mkDirOrFail(BatchTask.successDir.toFile());
            FileUtil.mkDirOrFail(BatchTask.failureDir.toFile());
            FileUtil.mkDirOrFail(BatchTask.garbageDir.toFile());

            selectShowDatabases();
        } catch (Exception e) {
            httpClient.close();
            throw e;
        }
    }

    @PreDestroy
    public void shutdown() throws IOException {
        httpClient.close();
    }

    /**
     * Пишем события в clickhouse отложенно (раз в X секунд, батчами)
     * по мере их накопления.
     * NOTE: все события должны иметь один тип clazz, мы не проверяем в runtime тип
     */
    public <T extends DbEvent> void save(Class<T> clazz, List<T> events) {
        if (!isEnabledWrite()) {
            return;
        }
        ConcurrentLinkedQueue<DbEvent> queue = writeCache.computeIfAbsent(clazz, aClass -> new ConcurrentLinkedQueue<DbEvent>());
        notNull(queue, "unsupported type of queue %s", clazz);
        queue.addAll(events);
    }

    public <T extends DbEvent> void save(T event) {
        if (!isEnabledWrite()) {
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace("save(): {}", event);
        }
        Class<? extends DbEvent> clazz = event.getClass();
        ConcurrentLinkedQueue<DbEvent> queue = writeCache.computeIfAbsent(clazz, aClass -> new ConcurrentLinkedQueue<DbEvent>());
        notNull(queue, "unsupported type of queue %s", clazz);
        queue.add(event);
    }

    private boolean isEnabledWrite() {
        return triggerDelay != 0 && triggerBatchSize != 0;
    }


    private void selectShowDatabases() {
        List<String> databases = new ArrayList<>();
        clickTemplate.query("select 1;", rs -> {
            databases.add(rs.getString(1));
        });
    }

    private boolean isBatchInserted(int batchId, String clickhouseTable) {
        try {
            boolean[] inserted = {false};
            clickTemplate.query("select count() > 0 from " + clickhouseDb + "." + clickhouseTable + " where eventDate >= yesterday() and batchId = " + batchId + ";", rs -> {
                inserted[0] = rs.getBoolean(1);
            });
            return inserted[0];
        } catch (Throwable t) {
            log.error("selectBatch():", t);
            return false;
        }
    }

    /**
     * Пишем все события которые накопились на данный момент в базу,
     * надо писать пачками от 10к штук
     */
    @Scheduled(fixedRateString = "${clickhouse.queueCheckFixedDelay}")
    public synchronized void flushToDb() {
        if (!isEnabledWrite()) {
            return;
        }
        writeImpInBatch();
    }

    private void writeImpInBatch() {
        try {
            for (Map.Entry<Class, ConcurrentLinkedQueue<DbEvent>> queueEntry : writeCache.entrySet()) {
                ConcurrentLinkedQueue<? extends DbEvent> impEvents = queueEntry.getValue();
                final int batchSize = impEvents.size();
                if (batchSize == 0) {
                    continue;
                }

                Class<? extends DbEvent> clazz = impEvents.peek().getClass();
                DbEvent evt;
                while ((evt = impEvents.poll()) != null) {
                    insertCount.incrementAndGet();
                    BatchWriter insertBuilder = getBatch(clazz);
                    insertBuilder.push(evt);

                    boolean triggerBatch = insertBuilder.getSize() > triggerBatchSize;
                    boolean triggerTime = !DateUtil.checkNoTimeout(insertBuilder.getCreationTs(), triggerDelay);
                    if (triggerBatch || triggerTime) {
                        log.debug("writeImpInBatch(): {} triggerBatch: {}/{}#queued, triggerTime: {}",
                                queueEntry.getKey().getSimpleName(), triggerBatch, batchSize, triggerTime);
                        insertBuilder.finish();
                        String insertStatement = insertBuilder.getStatement();
                        batches.remove(clazz);
                        if (log.isTraceEnabled()) {
                            log.trace("saving query: {}", insertStatement);
                        }
                        try {
                            saveAndSend(insertBuilder, insertStatement);
                        } catch (Exception e) {
                            log.error("failed to insert batch#", insertCount.get(), e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("error building batch sql", e);
        }
    }

    @NotNull
    private BatchWriter<? extends DbEvent> getBatch(Class clazz) {
        return batches.computeIfAbsent(
                clazz,
                aClass -> new BatchWriter(
                        UUID.randomUUID().hashCode(),
                        aClass,
                        buffers.computeIfAbsent(aClass, aClass1 -> new StringBuilder(triggerBatchSize)),
                        annotationScanner,
                        now()
                )
        );
    }

    private CompletableFuture<BatchTask.Status> saveAndSend(BatchWriter batchWriter, String statement) throws IOException {
        BatchTask batchTask = new BatchTask(batchWriter.getBatchId(), batchWriter.getTable(), statement);
        CompletableFuture<BatchTask.Status> future = send(statement, batchTask);
        future.thenAccept(status -> {
            switch (status) {
                case SUCCESS:
                    batchTask.markSuccess();
                    break;
                case FAILURE:
                    batchTask.markFailure();
                    break;
                case PENDING:
                    batchTask.markPending();
                    break;
            }
        });
        batchTask.writeOnDisk();
        return future;
    }

    private CompletableFuture<BatchTask.Status> send(String statement, BatchTask batchTask) {
        CompletableFuture<BatchTask.Status> future;

        if (networkActive.compareAndSet(false, true)) {
            future = httpClient.preparePost(clickhouseHttpUrl)
                    .setHeader("Content-Type", "text/plain; charset=UTF-8")
                    .setBody(statement)
                    .execute(new AsyncCompletionHandler<BatchTask.Status>() {
                                 @Override
                                 public BatchTask.Status onCompleted(Response response) {
                                     if (response.getStatusCode() != 200) {
                                         log.error("inserted batch#{}: http code: {} db error: {}",
                                                 insertCount.get(),
                                                 response.getStatusCode(), response.getResponseBody());

                                         return BatchTask.Status.FAILURE;
                                     } else {
                                         if (log.isInfoEnabled()) {
                                             log.info("inserted batch#{}: {} chars", insertCount.get(), statement.length());
                                         }
                                         return BatchTask.Status.SUCCESS;
                                     }
                                 }
                             }
                    ).toCompletableFuture().handle((status, err) -> {
                        networkActive.set(false);
                        if (err != null) {
                            log.error("send():", err);
                            return BatchTask.Status.FAILURE;
                        } else {
                            return status;
                        }
                    });
        } else {
            future = CompletableFuture.completedFuture(BatchTask.Status.PENDING);
        }
        return future;
    }

    public <T extends DbEvent> RowMapper<T> getRowMapper(Class<T> clazz) {
        return new ChAnnotationScanner.DbEventMapper(clazz, annotationScanner);
    }

    @Scheduled(fixedRateString = "${clickhouse.insert.pending.sendIntervalMs:500}")
    public synchronized void sendPending() {
        try {
            if (!isEnabledWrite()) {
                return;
            }
            if (networkActive.get()) {
                return;
            }
            File[] batchTasks = BatchTask.listPending();
            for (File f : batchTasks) {
                if (networkActive.get()) {
                    return;
                }
                BatchTask task = new BatchTask(f);
                send(new String(FileUtil.readFileFast(task.getFile().getAbsolutePath()), StandardCharsets.UTF_8), task).thenAccept(status -> {
                    switch (status) {
                        case SUCCESS:
                            task.markSuccess();
                            break;
                        case FAILURE:
                            task.markFailure();
                            break;
                        case PENDING:
                            task.markPending();
                            break;
                    }
                }).get();
            }
        } catch (Throwable t) {
            log.error("sendPending(): ", t);
        }
    }

    @Scheduled(fixedRateString = "${clickhouse.insert.success.cleanupIntervalMs:600000}")
    public synchronized void cleanup() {
        try {
            if (!isEnabledWrite()) {
                return;
            }
            File[] files = BatchTask.listSuccess();
            for (File f : files) {
                if (!DateUtil.checkNoTimeout(f.lastModified(), maxSuccessAgeHours, TimeUnit.HOURS)) {
                    f.delete();
                }
            }
        } catch (Throwable t) {
            log.error("cleanup(): ", t);
        }
    }

    @Scheduled(cron = "${clickhouse.insert.failed.retryCron:0 0 0 * * *}")
    public synchronized void retry() {
        try {
            if (!isEnabledWrite()) {
                return;
            }
            File[] batchTasks = BatchTask.listFailure();
            for (File f : batchTasks) {
                if (networkActive.get()) {
                    return;
                }
                BatchTask task = new BatchTask(f);
                if (isBatchInserted(task.id, task.table)) {
                    task.markSuccess();
                } else {
                    send(new String(FileUtil.readFileFast(task.getFile().getAbsolutePath()), StandardCharsets.UTF_8), task).thenAccept(status -> {
                        switch (status) {
                            case SUCCESS:
                                task.markSuccess();
                                break;
                            case FAILURE:
                                task.markGarbage();
                                break;
                            case PENDING:
                                task.markFailure();
                                break;
                        }
                    }).get();
                }
            }
        } catch (Throwable t) {
            log.error("retry(): ", t);
        }
    }

    @NotNull
    private String[] getList(Path writingDir) {
        return writingDir.toFile().list();
    }

    public int getWritingCount() {
        return getList(BatchTask.writingDir).length;
    }

    public int getPendingCount() {
        return getList(BatchTask.pendingDir).length;
    }

    public int getSuccessCount() {
        return getList(BatchTask.successDir).length;
    }

    public int getFailureCount() {
        return getList(BatchTask.failureDir).length;
    }

    public int getGarbageCount() {
        return getList(BatchTask.garbageDir).length;
    }

    private static class BatchTask {
        private static final Path writingDir = Paths.get("data", "clickhouse", "writing");
        private static final Path pendingDir = Paths.get("data", "clickhouse", "pending");
        private static final Path successDir = Paths.get("data", "clickhouse", "success");
        private static final Path failureDir = Paths.get("data", "clickhouse", "failure");
        private static final Path garbageDir = Paths.get("data", "clickhouse", "garbage");

        private static final String prefix = "batch_";

        private final int id;
        private final String fileName;
        private final String table;
        private String content;

        private volatile Path targetDir;
        private volatile File file;

        BatchTask(int id, String table, String content) {
            this.id = id;
            this.table = table;
            this.fileName = prefix + id + "_" + table;
            this.content = content;
        }

        BatchTask(File file) {
            this.file = file;
            String id_table = file.getName().substring(prefix.length());
            int separatorPos = id_table.indexOf('_');
            this.id = Integer.parseInt(id_table.substring(0, separatorPos));
            this.table = id_table.substring(separatorPos + 1);
            this.fileName = file.getName();
        }

        void writeOnDisk() throws IOException {
            File file = writingDir.resolve(fileName).toFile();
            FileUtil.writeFile(file, content);
            this.file = file;
            tryRename();
        }

        private synchronized void tryRename() {
            if (file == null || targetDir == null) {
                return;
            }

            File dest = targetDir.resolve(file.getName()).toFile();
            boolean success = file.renameTo(dest);
            if (!success) {
                log.error("tryRename(): failed to move file {} to {}", file, dest);
            }

            file = null;
            targetDir = null;
        }

        void markPending() {
            targetDir = pendingDir;
            tryRename();
        }

        void markSuccess() {
            targetDir = successDir;
            tryRename();
        }

        void markFailure() {
            targetDir = failureDir;
            tryRename();
        }

        void markGarbage() {
            targetDir = garbageDir;
            tryRename();
        }

        public File getFile() {
            return file;
        }

        static File[] listPending() {
            File[] pending = pendingDir.toFile().listFiles();

            Arrays.sort(pending, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));

            return pending;
        }

        static File[] listSuccess() {
            return successDir.toFile().listFiles();
        }

        static File[] listFailure() {
            File[] failure = failureDir.toFile().listFiles();

            Arrays.sort(failure, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));

            return failure;
        }

        enum Status {
            PENDING, FAILURE, SUCCESS
        }

    }
}
