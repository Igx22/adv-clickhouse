package adv.clickhouse.dao;

import adv.clickhouse.TestEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import static org.junit.Assert.assertEquals;


public class ClickHouseDaoTest {

    @InjectMocks
    ClickHouseDao clickHouseDao;

    @Mock
    JdbcTemplate jdbcTemplate;

    @Mock
    ThreadPoolTaskScheduler scheduler;

    @Before
    public void init() {
        clickHouseDao = new ClickHouseDao(scheduler);
        MockitoAnnotations.initMocks(this);
    }


    public void test() throws Exception {
        clickHouseDao.setTriggerBatchSize(1000);
        clickHouseDao.setTriggerDelay(100000);
        clickHouseDao.setClickhouseDb("db0");
        clickHouseDao.setClickhousePkg("adv.clickhouse");
        clickHouseDao.init();

        for (int i = 0; i < 36398030; i++) {
            clickHouseDao.save(new TestEvent());
        }
        assertEquals(2, clickHouseDao.batches.get(TestEvent.class).size());
        clickHouseDao.shutdown();
    }
}