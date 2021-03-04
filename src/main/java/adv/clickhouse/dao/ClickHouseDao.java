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
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.support.CronTrigger;
import org.springframework.stereotype.Component;

import javax.annotation.Nullable;
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
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static adv.util.Check.notNull;
import static adv.util.DateUtil.now;

/*
Про глюки с clickhouse:
--
В CH нет четкой логики на какую ошибку повторные вставки можно делать а на какую нельзя.
Путем проб и ошибок мы пришли к тому что надо ретраить если любая ошибка не связанная со стукторой запроса (т.е. не кривой sql = Code: 252)
Это более менее работало последние год-два.

Что сломалось: стейдж втыкает данные в clickhouse, получает какой-то http код ошибки (не 200) c текстом ошибки (DB::Exception Too many parts)
и согласно концепции надо повторить insert - далее идет логика ретрая
Но из за глюков запрос на самом деле попадает как кусочек на диск и мержится кликхаусом (хотя мы и получили ошибку).
Итог = база переполняется запросами на мерж.

Как с этим борются в php либе:
--
В каждую таблицу добавляют колонку batchId, и через день или час делают select в базу на предмет попала ли хотя бы 1 строчка с этим batchId в базу.
Если не попала - по крону пытается сделать повторный insert (это один вариант в некоторых проектах)
Либо руками запускают скрипт который пытается сделать повторный insert (это другой вариант в некоторых проектах)
Ключевая мысль здесь - не верить ни http коду ошибки, ни тексту ошибки, вообще ничему кроме кода 200 (=успешная вставка)


Как надо сделать в ClickhouseDao:
--
1) при даунтайме базы core должен продолжать работать пока не кончится место на диске
т.е. если скорости диска хватает - все должно работать

2) если ch выдает сложные и хитрые ошибки - мы не должны их анализировать, т.к. коды ответа могут нас обманывать,
а повторные попытки вставить данные могут привести к дублям.
т.е. стратегия досылки данных - втыкать их потом руками сисадмина чз скрипт

3) мы должны работать даже с очень большими строчками, т.е. уметь адекватно оценивать размер одного батча для вставки
- большой батч = плохо. не нужно копить слишком много в памяти,
ранее мы утыкались в размер страницы очереди которую использовали
также мы можем получать таймауты при операциях работы с ch.
- маленький батч = плохо, это значит втыкать слишком часто т.к. при наличии 4 нодов и N таблиц мы можем делать 4*N вставок

4) ? использовать идентификатор батча в каждой строке данных чтобы не втыкать дубли данных

5) размер батча или частота втыкания может быть разной для разных таблиц

6) разные таблицы мы можем хотеть втыкать в разные ноды сервера

реализация:
--
для каждой таблицы по триггеру (Х секунд=60 или Y мегабайт=200) мы формируем батч с уникальным ID (например hash64(uuidv4)).
батч пишем на диск и потом пытаемся послать в CH в один поток.
если успешно послали - переименовываем файл на диске в посланный. держим на диске не более чем Z файлов.
чтобы оценить мегабайты - держим в памяти 100 рандомных строк этой таблицы (постоянно пополняем их),
считаем средний размер строки, считаем сколько строк надо на 1мб, умножаем это на 200.

если получилось (http 200) - все окей.
если не получилось - не пытаемся анализировать ошибку - скидываем его на диск в файл.
такие батчи через день должны втыкаться по крону.

7) желательно хранить каждый батч на диске как csv/sql файл
желательно писать в tmp файл а потом переименовывать чтобы не получать битые файлы при kill -9 ?
пример формата именования файлов batch#8129830_table1_181107_163925.857.sql

Пример ошибки too many parts
2019.11.26 14:08:01.956115 [ 15121 ] {d77c1957-694f-47b3-b86e-f9f56259ae9f} <Error> executeQuery: Code: 252, e.displayText() = DB::Exception: Too many parts (301). Merges are processing significantly slower than inserts. (version 19.9.4.34 (official build)) (from [::ffff:10.85.0.181]:52676) (in query: INSERT INTO ...

*/
@SuppressWarnings("unchecked")
public class ClickHouseDao {
    private static final Logger log = LoggerFactory.getLogger(ClickHouseDao.class);

    @Autowired
    @Qualifier("clickHouseJdbcTemplate")
    JdbcTemplate clickTemplate;

    private TaskScheduler scheduler;

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

    @Value("${clickhouse.insertTimeoutSec:300}")
    private int insertTimeoutSec;

    @Value("${clickhouse.insert.pending.sendIntervalMs:500}")
    private int sendPendingRate;

    @Value("${clickhouse.queueCheckFixedDelay:30000}")
    private int flushToDbRate;

    @Value("${clickhouse.insert.success.cleanupIntervalMs:600000}")
    private int cleanupRate;

    @Value("${clickhouse.insert.failed.retryCron:0 0 0 * * *}")
    String retryCron;

    private AsyncHttpClient httpClient;

    // TODO: хранить BatchWriter вместо событий на запись
    // Очереди событий каждого типа
    private Map<Class, ConcurrentLinkedQueue<DbEvent>> writeCache = new ConcurrentHashMap<>();
    // Локи
    private Map<Class, Lock> writeLocks = new ConcurrentHashMap<>();

    private ChAnnotationScanner annotationScanner;

    private AtomicInteger insertCount = new AtomicInteger(0);

    private AtomicBoolean networkActive = new AtomicBoolean(false);

    private Map<Class, StringBuilder> buffers = new HashMap<>();

    protected Map<Class<? extends DbEvent>, Deque<BatchWriter<? extends DbEvent>>> batches = new HashMap<>();

    public ClickHouseDao(TaskScheduler clickhousePool) {
        this.scheduler = clickhousePool;
    }


    public void setTriggerDelay(int triggerDelay) {
        this.triggerDelay = triggerDelay;
    }

    public void setTriggerBatchSize(int triggerBatchSize) {
        this.triggerBatchSize = triggerBatchSize;
    }

    public void setClickhouseDb(String clickhouseDb) {
        this.clickhouseDb = clickhouseDb;
    }

    public void setClickhousePkg(String clickhousePkg) {
        this.clickhousePkg = clickhousePkg;
    }

    @PostConstruct
    public void init() throws IllegalAccessException, InvocationTargetException, IntrospectionException, IOException {
        log.debug("init AsyncHttpClient");
        AsyncHttpClientConfig asyncHttpClientConfig = new DefaultAsyncHttpClientConfig.Builder()
                .setThreadFactory(ExecutorUtil.createNamedThreadFactory("clickhouse-dao-ahc-"))
                .setIoThreadsCount(ioThreads)
                .setRequestTimeout(insertTimeoutSec * 1000)
                .setReadTimeout(insertTimeoutSec * 1000)
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

        scheduler.scheduleAtFixedRate(this::jobSendPending, sendPendingRate);
        scheduler.scheduleAtFixedRate(this::jobFlushToDb, flushToDbRate);
        scheduler.scheduleAtFixedRate(this::jobCleanup, cleanupRate);
        scheduler.schedule(this::jobRetry, new CronTrigger(retryCron));
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
        Lock lock = writeLocks.computeIfAbsent(clazz, aClass -> new ReentrantLock());
        if (lock.tryLock()) {
            try {
                drainToBatch(clazz, queue);
                drainToBatch(clazz, events);
            } finally {
                lock.unlock();
            }
        } else {
            queue.addAll(events);
        }
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
        Lock lock = writeLocks.computeIfAbsent(clazz, aClass -> new ReentrantLock());
        if (lock.tryLock()) {
            try {
                drainToBatch(clazz, queue);
                drainToBatch(clazz, List.of(event));
            } finally {
                lock.unlock();
            }
        } else {
            queue.add(event);
        }
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
    public synchronized void jobFlushToDb() {
        log.debug("jobFlushToDb(): started");
        if (!isEnabledWrite()) {
            return;
        }
        try {
            Set<Class<? extends DbEvent>> s = new HashSet<>(batches.keySet());
            for (Class<? extends DbEvent> clazz : s) {
                Lock lock = writeLocks.get(clazz);
                lock.lock();
                try {
                    flushIfNeeded(clazz);
                } finally {
                    lock.unlock();
                }
            }
        } catch (Throwable t) {
            log.error("writeImpInBatch(): ", t);
        }
        log.debug("jobFlushToDb(): finished");
    }

    private void drainToBatch(Class<? extends DbEvent> clazz, ConcurrentLinkedQueue<? extends DbEvent> queue) {
        try {
            DbEvent evt;
            while ((evt = queue.poll()) != null) {
                appendToBatch(clazz, evt);
            }
        } catch (Exception e) {
            log.error("error building batch sql", e);
        }
    }

    private void drainToBatch(Class<? extends DbEvent> clazz, List<? extends DbEvent> events) {
        try {
            for (int i = 0; i < events.size(); i++) {
                appendToBatch(clazz, events.get(i));
            }
        } catch (Exception e) {
            log.error("error building batch sql", e);
        }
    }

    /**
     * Проверяем не надо ли флашнуть batch
     * @param evtClazz класс с котором ассоциирован батч
     */
    private void flushIfNeeded(@NotNull Class<? extends DbEvent> evtClazz) {
        Deque<BatchWriter<? extends DbEvent>> batches = getBatches(evtClazz);
        BatchWriter<? extends DbEvent> batchWriter;
        boolean lastBatch = false;
        while ((batchWriter = batches.peekFirst()) != null && !lastBatch) {
            if (batches.size() == 1) lastBatch = true;
            boolean triggerBatch = batchWriter.getSize() >= triggerBatchSize;
            boolean triggerTimeAndNotEmpty = !DateUtil.checkNoTimeout(batchWriter.getCreationTs(), triggerDelay) && batchWriter.hasData();
            if (triggerBatch || triggerTimeAndNotEmpty || batches.size() > 1) {
                log.debug("writeImpInBatch(): class: {} triggerBySize: {}, triggerByTime: {}", evtClazz.getSimpleName(), triggerBatch, triggerTimeAndNotEmpty);
                batchWriter.finish();
                String insertStatement = batchWriter.getStatement();
                batches.removeFirst();
                if (log.isTraceEnabled()) {
                    log.trace("saving query: {}", insertStatement);
                }
                try {
                    saveAndSend(batchWriter, insertStatement);
                    insertCount.incrementAndGet();
                } catch (Exception e) {
                    log.error("failed to insert batch#{}", insertCount.get(), e);
                }
            }
        }
    }

    private void appendToBatch(@NotNull Class<? extends DbEvent> evtClazz, @Nullable DbEvent evt) {
        BatchWriter batchWriter = getCurrentBatch(evtClazz, false);
        if (evt != null) {
            if (!batchWriter.push(evt)) {
                batchWriter = getCurrentBatch(evtClazz, true);
                batchWriter.push(evt);
            }
        }
    }

    @NotNull
    private BatchWriter<? extends DbEvent> getCurrentBatch(Class clazz, boolean forceNew) {
        Deque<BatchWriter<? extends DbEvent>> queue = batches.computeIfAbsent(
                clazz,
                aClass -> new LinkedList<>());
        if (queue.isEmpty() || forceNew) {
            queue.offerLast(new BatchWriter(
                    UUID.randomUUID().hashCode(),
                    clazz,
                    buffers.computeIfAbsent(clazz, aClass -> new StringBuilder(triggerBatchSize)),
                    annotationScanner,
                    now()
            ));
        }
        return queue.peekLast();
    }

    private Deque<BatchWriter<? extends DbEvent>> getBatches(Class clazz) {
        return batches.computeIfAbsent(
                clazz,
                aClass -> new LinkedList<>());
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
        log.debug("send(): batch {}/{}", batchTask.table, batchTask.id);
        if (networkActive.compareAndSet(false, true)) {
            log.debug("send(): batch {}/{} sending", batchTask.table, batchTask.id);
            future = httpClient.preparePost(clickhouseHttpUrl)
                    .setHeader("Content-Type", "text/plain; charset=UTF-8")
                    .setHeader("User-Agent", "batch=" + batchTask.table + '/' + batchTask.id)
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
                        log.debug("send(): batch {}/{} done", batchTask.table, batchTask.id);
                        networkActive.set(false);
                        if (err != null) {
                            log.error("send():", err);
                            return BatchTask.Status.FAILURE;
                        } else {
                            return status;
                        }
                    });
        } else {
            log.debug("send(): batch {}/{}, client busy", batchTask.table, batchTask.id);
            future = CompletableFuture.completedFuture(BatchTask.Status.PENDING);
        }
        return future;
    }

    public <T extends DbEvent> RowMapper<T> getRowMapper(Class<T> clazz) {
        return new ChAnnotationScanner.DbEventMapper(clazz, annotationScanner);
    }

    public synchronized void jobSendPending() {
        log.debug("jobSendPending(): started");
        try {
            if (!isEnabledWrite()) {
                return;
            }
            if (networkActive.get()) {
                log.debug("jobSendPending(): client busy 1");
                return;
            }
            File[] batchTasks = BatchTask.listPending();
            for (File f : batchTasks) {
                if (networkActive.get()) {
                    log.debug("jobSendPending(): client busy 2");
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
        log.debug("jobSendPending(): finished");
    }

    public synchronized void jobCleanup() {
        log.debug("jobCleanup(): started");
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
        log.debug("jobCleanup(): finished");
    }

    public void scheduleRetry() {
        scheduler.schedule(this::jobRetry, Instant.now());
    }

    private void jobRetry() {
        log.debug("jobRetry(): started");
        try {
            if (!isEnabledWrite()) {
                return;
            }
            File[] batchTasks = BatchTask.listFailure();
            while (batchTasks.length > 0) {
                retryBatches(batchTasks);
                batchTasks = BatchTask.listFailure();
            }
        } catch (Throwable t) {
            log.error("retry(): ", t);
        }
        log.debug("jobRetry(): finished");
    }

    private void retryBatches(File[] batchTasks) throws InterruptedException, ExecutionException, IOException {
        for (File f : batchTasks) {
            while (networkActive.get()) {
                ThreadUtil.sleep(500);
            }
            BatchTask task = new BatchTask(f);
            if (isBatchInserted(task.id, task.table)) {
                log.debug("retryBatches(): batch {}/{} already inserted",  task.table, task.id);
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

    public boolean isNetworkActive() {
        return networkActive.get();
    }

    public Map<String, Integer> getCacheSizes() {
        return writeCache.entrySet().stream().collect(Collectors.toMap(e-> e.getKey().getSimpleName(), e -> e.getValue().size()));
    }

    public Map<BatchTask.Status, List<BatchTask>> getAllTasks() {
        Map<BatchTask.Status, List<BatchTask>> result = new HashMap<>();
        result.put(BatchTask.Status.PENDING, Arrays.stream(BatchTask.listPending()).map(BatchTask::new).collect(Collectors.toList()));
        result.put(BatchTask.Status.SUCCESS, Arrays.stream(BatchTask.listSuccess()).map(BatchTask::new).collect(Collectors.toList()));
        result.put(BatchTask.Status.FAILURE, Arrays.stream(BatchTask.listFailure()).map(BatchTask::new).collect(Collectors.toList()));
        result.put(BatchTask.Status.WRITING, Arrays.stream(BatchTask.listWriting()).map(BatchTask::new).collect(Collectors.toList()));
        result.put(BatchTask.Status.GARBAGE, Arrays.stream(BatchTask.listGarbage()).map(BatchTask::new).collect(Collectors.toList()));
        return result;
    }

    public Map<String, Integer> getBatchWritersSizes() {
        return batches.entrySet().stream().collect(Collectors.toMap(e-> e.getKey().getSimpleName(), e-> e.getValue().size()));
    }

    public static class BatchTask {
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

        public String getTable() {
            return table;
        }

        private synchronized void tryRename() {
            if (file == null || targetDir == null) {
                return;
            }

            File dest = targetDir.resolve(file.getName()).toFile();
            log.debug("tryRename(): moving file {} to {}", file, dest);
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

        static File[] listGarbage() {
            File[] garbage = garbageDir.toFile().listFiles();
            Arrays.sort(garbage, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));
            return garbage;
        }

        static File[] listWriting() {
            File[] writing = garbageDir.toFile().listFiles();
            Arrays.sort(writing, (a, b) -> Long.compare(b.lastModified(), a.lastModified()));
            return writing;
        }

        public enum Status {
            PENDING, FAILURE, SUCCESS, WRITING, GARBAGE
        }
    }
}
