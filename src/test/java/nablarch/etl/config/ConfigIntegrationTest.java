package nablarch.etl.config;

import javax.batch.runtime.BatchStatus;
import javax.batch.runtime.JobExecution;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import mockit.Deencapsulation;
import nablarch.etl.integration.EtlIntegrationTest;
import nablarch.fw.batch.ee.initializer.RepositoryInitializer;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * コンフィグ設定の結合テスト。
 */
public class ConfigIntegrationTest {

    @Before
    public void setUp() throws Exception {
        EtlConfigProvider.isInitialized = false;
        Deencapsulation.setField(RepositoryInitializer.class, "isInitialized", false);
    }

    @After
    public void tearDown() throws Exception {
        Deencapsulation.setField(RepositoryInitializer.class, "isInitialized", false);
    }

    /**
     * JavaBatchを実行し、Batchlet/ItemReader/ItemWriterに
     * インジェクションした{@link EtlConfigProvider}から設定値が取得できること。
     */
    @Test
    public void testJob1() {
        JobExecution execution = EtlIntegrationTest.startJob("config-integration-test-job1");
        assertThat(execution.getBatchStatus(), is(BatchStatus.COMPLETED));
    }
}
