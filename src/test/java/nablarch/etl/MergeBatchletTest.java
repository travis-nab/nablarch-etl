package nablarch.etl;

import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.batch.runtime.context.JobContext;
import javax.batch.runtime.context.StepContext;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hamcrest.Matchers;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.db.statement.exception.SqlStatementException;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionFactory;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.fw.batch.ee.progress.BasicProgressManager;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;
import nablarch.test.support.log.app.OnMemoryLogWriter;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import mockit.Expectations;
import mockit.Mocked;

/**
 * {@link MergeBatchlet}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class MergeBatchletTest {

    @ClassRule
    public static SystemRepositoryResource resource = new SystemRepositoryResource("db-default.xml");

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    TransactionManagerConnection connection;

    @Mocked
    private JobContext mockJobContext;

    @Mocked
    private StepContext mockStepContext;
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(EtlMergeEntity.class);
        VariousDbTestHelper.createTable(EtlMergeInputWorkEntity.class);
        VariousDbTestHelper.createTable(EtlMergeMultiKey.class);
        VariousDbTestHelper.createTable(EtlMergeMultiKeyWork.class);

        TransactionFactory transactionFactory = resource.getComponent("jdbcTransactionFactory");
        TransactionContext.setTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY,
                transactionFactory.getTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        TransactionContext.removeTransaction();
    }

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = resource.getComponent("connectionFactory");
        connection = connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(connection);

        // -------------------------------------------------- setup objects that is injected
        new Expectations() {{
            mockStepContext.getStepName();
            result = "test-step";
            mockJobContext.getJobName();
            result = "test-job";
        }};

        // clear resource
        OnMemoryLogWriter.clear();
    }

    @After
    public void tearDown() throws Exception {
        connection.rollback();
        connection.terminate();
        DbConnectionContext.removeConnection();

        OnMemoryLogWriter.clear();
    }


    @Test
    public void beanSetNull_shouldThrowException() throws Exception {

        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("bean is required. jobId = [test-job], stepId = [test-step]");
        sut.process();
    }

    @Test
    public void sqlIdSetNull_shouldThroewException() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("sqlId is required. jobId = [test-job], stepId = [test-step]");
        sut.process();
    }

    @Test
    public void mergeOnColumnsSetNull_shouldThrowException() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setSqlId("test");

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("mergeOnColumns is required. jobId = [test-job], stepId = [test-step]");
        sut.process();
    }

    @Test
    public void updateSizeSetNull_shouldThrowException() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        stepConfig.initialize();
        final DbToDbStepConfig.UpdateSize size = new DbToDbStepConfig.UpdateSize();
        stepConfig.setUpdateSize(size);

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("updateSize.size is required. jobId = [test-job], stepId = [test-step]");
        sut.process();
    }

    @Test
    public void workTableBeanSetNull_shouldThrowException() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        stepConfig.initialize();
        final DbToDbStepConfig.UpdateSize size = new DbToDbStepConfig.UpdateSize();
        size.setSize(1000);
        stepConfig.setUpdateSize(size);

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("updateSize.bean is required. jobId = [test-job], stepId = [test-step]");
        sut.process();
    }

    /**
     * 更新サイズが不正な場合、例外が送出されること。
     */
    @Test
    public void testInvalidUpdateSize() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        stepConfig.initialize();
        final DbToDbStepConfig.UpdateSize size = new DbToDbStepConfig.UpdateSize();
        size.setSize(0);
        stepConfig.setUpdateSize(size);

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("updateSize.size must be greater than 0. "
                + "jobId = [test-job], stepId = [test-step], size = [0]");
        sut.process();

    }

    /**
     * マージ処理が正常に終了すること。
     */
    @Test
    public void mergeSuccess() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeInputWorkEntity(1L, 1L, "name1", "address1"),
                new EtlMergeInputWorkEntity(2L, 2L, "name2", "address2"),
                new EtlMergeInputWorkEntity(3L, 3L, "name3", "address3"),
                new EtlMergeInputWorkEntity(4L, 4L, "name4", "address4"),
                new EtlMergeInputWorkEntity(5L, 5L, "name5", "address5")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeEntity(3L, "3", "3"),
                new EtlMergeEntity(6L, "name6", "address6")
        );

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL");
        stepConfig.initialize();

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        sut.process();
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeEntity> result = VariousDbTestHelper.findAll(EtlMergeEntity.class, "userId");
        assertThat("変更なし1、更新1、追加4で6レコード存在する", result.size(), is(6));

        for (int i = 0; i < 6; i++) {
            final EtlMergeEntity entity = result.get(i);
            int index = i + 1;
            assertThat(entity.userId, is((long) index));
            assertThat(entity.name, is("name" + index));
            assertThat(entity.address, is("address" + index));
        }

        OnMemoryLogWriter.assertLogContains("writer.sql", "update count = [5]");

        final List<String> messages = OnMemoryLogWriter.getMessages("writer.progress");
        assertThat(messages, Matchers.contains(
                containsString("-INFO- job name: [test-job] step name: [test-step] input count: [5]"),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step]"),
                        containsString("remaining count: [0]")
                )
        ));
    }

    /**
     * 1回のSQL実行で処理するサイズが指定された場合に、マージ処理が正常に終了すること。
     */
    @Test
    public void mergeSuccessUsingSplit() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeInputWorkEntity(1L, 1L, "name1", "address1"),
                new EtlMergeInputWorkEntity(2L, 2L, "name2", "address2"),
                new EtlMergeInputWorkEntity(3L, 3L, "name3", "address3"),
                new EtlMergeInputWorkEntity(4L, 4L, "name4", "address4"),
                new EtlMergeInputWorkEntity(5L, 5L, "name5", "address5")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeEntity(3L, "3", "3"),
                new EtlMergeEntity(6L, "name6", "address6")
        );

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        final DbToDbStepConfig.UpdateSize updateSize = new DbToDbStepConfig.UpdateSize();
        updateSize.setSize(2);
        updateSize.setBean(EtlMergeInputWorkEntity.class);
        stepConfig.setUpdateSize(updateSize);
        stepConfig.initialize();

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        sut.process();
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeEntity> result = VariousDbTestHelper.findAll(EtlMergeEntity.class, "userId");
        assertThat("変更なし1、更新1、追加4で6レコード存在する", result.size(), is(6));

        for (int i = 0; i < 6; i++) {
            final EtlMergeEntity entity = result.get(i);
            int index = i + 1;
            assertThat(entity.userId, is((long) index));
            assertThat(entity.name, is("name" + index));
            assertThat(entity.address, is("address" + index));
        }

        assertSqlExecutionAndCommitTimes(
                "update count = [2]", COMMIT_MSG,
                "update count = [2]", COMMIT_MSG,
                "update count = [1]", COMMIT_MSG);

        final List<String> messages = OnMemoryLogWriter.getMessages("writer.progress");
        assertThat(messages, Matchers.contains(
                containsString("-INFO- job name: [test-job] step name: [test-step] input count: [5]"),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step] "),
                        containsString("remaining count: [3]")
                ),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step] "),
                        containsString("remaining count: [1]")
                ),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step] "),
                        containsString("remaining count: [0]")
                )
        ));
    }

    /**
     * 1回のSQL実行で処理するサイズが指定され、
     * 途中のSQL実行で処理件数が0の場合でも、
     * 最後までマージ処理が正常に終了すること。
     */
    @Test
    public void mergeSuccessUsingSplitWhenIncludeNoData() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeInputWorkEntity(1L, 1L, "name1", "address1"),
                new EtlMergeInputWorkEntity(2L, 2L, "name2", "address2"),
                new EtlMergeInputWorkEntity(3L, 3L, "name3", "address3"),
                new EtlMergeInputWorkEntity(4L, 4L, "name4", "address4"),
                new EtlMergeInputWorkEntity(5L, 5L, "name5", "address5")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeEntity(3L, "3", "3"),
                new EtlMergeEntity(6L, "name6", "address6")
        );

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE_AND_FILTER");
        final DbToDbStepConfig.UpdateSize updateSize = new DbToDbStepConfig.UpdateSize();
        updateSize.setSize(2);
        updateSize.setBean(EtlMergeInputWorkEntity.class);
        stepConfig.setUpdateSize(updateSize);
        stepConfig.initialize();

        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        sut.process();
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeEntity> result = VariousDbTestHelper.findAll(EtlMergeEntity.class, "userId");
        assertThat("変更なし2、更新0、追加3で5レコード存在する", result.size(), is(5));

        assertThat(result.get(0).userId, is(1L));
        assertThat(result.get(0).name, is("name1"));
        assertThat(result.get(0).address, is("address1"));
        assertThat(result.get(1).userId, is(2L));
        assertThat(result.get(1).name, is("name2"));
        assertThat(result.get(1).address, is("address2"));
        assertThat(result.get(2).userId, is(3L));
        assertThat(result.get(2).name, is("3"));
        assertThat(result.get(2).address, is("3"));
        assertThat(result.get(3).userId, is(5L));
        assertThat(result.get(3).name, is("name5"));
        assertThat(result.get(3).address, is("address5"));
        assertThat(result.get(4).userId, is(6L));
        assertThat(result.get(4).name, is("name6"));
        assertThat(result.get(4).address, is("address6"));

        assertSqlExecutionAndCommitTimes(
                "update count = [2]", COMMIT_MSG,
                "update count = [0]", COMMIT_MSG,
                "update count = [1]", COMMIT_MSG);

        final List<String> messages = OnMemoryLogWriter.getMessages("writer.progress");
        assertThat(messages, Matchers.contains(
                containsString("-INFO- job name: [test-job] step name: [test-step] input count: [5]"),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step] "),
                        containsString("remaining count: [3]")
                ),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step] "),
                        containsString("remaining count: [1]")
                ),
                allOf(
                        containsString("-INFO- job name: [test-job] step name: [test-step] "),
                        containsString("remaining count: [0]")
                )
        ));
    }

    /**
     * 1回のSQL実行で処理するサイズが指定され、ワークテーブルのデータがない場合に、マージ処理が正常に終了すること。
     */
    @Test
    public void mergeSuccessUsingSplitForNoWorkData() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.delete(EtlMergeInputWorkEntity.class);

        VariousDbTestHelper.setUpTable(
                new EtlMergeEntity(3L, "3", "3"),
                new EtlMergeEntity(6L, "name6", "address6")
        );

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Collections.singletonList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        final DbToDbStepConfig.UpdateSize updateSize = new DbToDbStepConfig.UpdateSize();
        updateSize.setSize(2);
        updateSize.setBean(EtlMergeInputWorkEntity.class);
        stepConfig.setUpdateSize(updateSize);
        stepConfig.initialize();
        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        sut.process();
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeEntity> result = VariousDbTestHelper.findAll(EtlMergeEntity.class, "userId");
        assertThat("変更なし2で2レコード存在する", result.size(), is(2));

        assertThat(result.get(0).userId, is(3L));
        assertThat(result.get(0).name, is("3"));
        assertThat(result.get(0).address, is("3"));
        assertThat(result.get(1).userId, is(6L));
        assertThat(result.get(1).name, is("name6"));
        assertThat(result.get(1).address, is("address6"));
    }

    /**
     * 複数の結合カラムを持つテーブルの場合でもマージ処理が正常に完了すること
     */
    @Test
    public void merge_multiJoinColumn() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeMultiKeyWork(1L, "1", "user1@mail.com", "user1"),
                new EtlMergeMultiKeyWork(2L, "1", "user1@mail.com2", "user1"),
                new EtlMergeMultiKeyWork(3L, "2", "user2@mail.com", "user2"),
                new EtlMergeMultiKeyWork(4L, "99", "user99@mail.com", "user99")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeMultiKey(1L, "user1@mail.com2", "更新前_user1"),
                new EtlMergeMultiKey(2L, "user2@mail.com2", "更新されない_user2"),
                new EtlMergeMultiKey(100L, "user100@mail.com", "user100")
        );

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeMultiKey.class);
        stepConfig.setMergeOnColumns(Arrays.asList("id", "mail_address"));
        stepConfig.setSqlId("SELECT_ALL");
        stepConfig.initialize();
        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        sut.process();
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeMultiKey> result = VariousDbTestHelper.findAll(EtlMergeMultiKey.class, "id", "mailAddress");
        assertThat(result, Matchers.contains(
                allOf(hasProperty("id", is(1L)), hasProperty("mailAddress", is("user1@mail.com")), hasProperty("name", is("user1"))),
                allOf(hasProperty("id", is(1L)), hasProperty("mailAddress", is("user1@mail.com2")), hasProperty("name", is("user1"))),
                allOf(hasProperty("id", is(2L)), hasProperty("mailAddress", is("user2@mail.com")), hasProperty("name", is("user2"))),
                allOf(hasProperty("id", is(2L)), hasProperty("mailAddress", is("user2@mail.com2")), hasProperty("name", is("更新されない_user2"))),
                allOf(hasProperty("id", is(99L)), hasProperty("mailAddress", is("user99@mail.com")), hasProperty("name", is("user99"))),
                allOf(hasProperty("id", is(100L)), hasProperty("mailAddress", is("user100@mail.com")), hasProperty("name", is("user100")))
        ));
        
        assertSqlExecutionAndCommitTimes("update count = [4]");
        final List<String> messages = OnMemoryLogWriter.getMessages("writer.progress");
        assertThat(messages, Matchers.contains(
                containsString("-INFO- job name: [test-job] step name: [test-step] input count: [4]"),
                containsString("remaining count: [0]")
        ));
    }

    /**
     * 1回のSQL実行で処理するサイズが指定され、複数の結合カラムを持つテーブルの場合でもマージ処理が正常に完了すること
     */
    @Test
    public void merge_multiJoinColumnUsingSplit() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeMultiKeyWork(1L, "1", "user1@mail.com", "user1"),
                new EtlMergeMultiKeyWork(2L, "1", "user1@mail.com2", "user1"),
                new EtlMergeMultiKeyWork(3L, "2", "user2@mail.com", "user2"),
                new EtlMergeMultiKeyWork(4L, "99", "user99@mail.com", "user99")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeMultiKey(1L, "user1@mail.com2", "更新前_user1"),
                new EtlMergeMultiKey(2L, "user2@mail.com2", "更新されない_user2"),
                new EtlMergeMultiKey(100L, "user100@mail.com", "user100")
        );

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeMultiKey.class);
        stepConfig.setMergeOnColumns(Arrays.asList("id", "mail_address"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        final DbToDbStepConfig.UpdateSize size = new DbToDbStepConfig.UpdateSize();
        size.setSize(2);
        size.setBean(EtlMergeMultiKeyWork.class);
        stepConfig.setUpdateSize(size);
        stepConfig.initialize();
        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        sut.process();
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeMultiKey> result = VariousDbTestHelper.findAll(EtlMergeMultiKey.class, "id", "mailAddress");
        assertThat(result, Matchers.contains(
                allOf(hasProperty("id", is(1L)), hasProperty("mailAddress", is("user1@mail.com")), hasProperty("name", is("user1"))),
                allOf(hasProperty("id", is(1L)), hasProperty("mailAddress", is("user1@mail.com2")), hasProperty("name", is("user1"))),
                allOf(hasProperty("id", is(2L)), hasProperty("mailAddress", is("user2@mail.com")), hasProperty("name", is("user2"))),
                allOf(hasProperty("id", is(2L)), hasProperty("mailAddress", is("user2@mail.com2")), hasProperty("name", is("更新されない_user2"))),
                allOf(hasProperty("id", is(99L)), hasProperty("mailAddress", is("user99@mail.com")), hasProperty("name", is("user99"))),
                allOf(hasProperty("id", is(100L)), hasProperty("mailAddress", is("user100@mail.com")), hasProperty("name", is("user100")))
        ));

        assertSqlExecutionAndCommitTimes(
                "update count = [2]", COMMIT_MSG,
                "update count = [2]", COMMIT_MSG);

        final List<String> messages = OnMemoryLogWriter.getMessages("writer.progress");
        assertThat(messages, Matchers.contains(
                containsString("input count: [4]"),
                containsString("remaining count: [2]"),
                containsString("remaining count: [0]")
        ));
    }

    /**
     * merge処理に失敗するケース
     */
    @Test
    public void mergeFailed() throws Exception {
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeInputWorkEntity(1L, 1L, "name1", "address1"),
                new EtlMergeInputWorkEntity(2L, 2L, "name2", "address2"),
                new EtlMergeInputWorkEntity(3L, 3L, "111111111111111111111111111111", "address3"),
                new EtlMergeInputWorkEntity(4L, 4L, "name4", "address4"),
                new EtlMergeInputWorkEntity(5L, 5L, "name5", "address5")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeEntity(3L, "3", "3"));

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Arrays.asList("user_id"));
        stepConfig.setSqlId("SELECT_ALL");
        stepConfig.initialize();
        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        try {
            sut.process();
            fail();
        } catch (SqlStatementException ignored) {
        }
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeEntity> result = VariousDbTestHelper.findAll(EtlMergeEntity.class, "userId");
        assertThat("SQL実行に失敗するので元の1レコードのみ", result.size(), is(1));
        assertThat(result.get(0).userId, is(3L));
        assertThat(result.get(0).name, is("3"));
        assertThat(result.get(0).address, is("3"));
    }

    /**
     * 1回のSQL実行で処理するサイズが指定され、かつmerge処理に失敗するケース
     */
    @Test
    public void mergeFailedUsingSplit() throws Exception {
        
        // -------------------------------------------------- setup table data
        VariousDbTestHelper.setUpTable(
                new EtlMergeInputWorkEntity(1L, 1L, "name1", "address1"),
                new EtlMergeInputWorkEntity(2L, 2L, "name2", "address2"),
                new EtlMergeInputWorkEntity(3L, 3L, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "address3"),
                new EtlMergeInputWorkEntity(4L, 4L, "name4", "address4"),
                new EtlMergeInputWorkEntity(5L, 5L, "name5", "address5")
        );

        VariousDbTestHelper.setUpTable(
                new EtlMergeEntity(3L, "3", "3"));

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Arrays.asList("user_id"));
        stepConfig.setSqlId("SELECT_ALL_WITH_RANGE");
        final DbToDbStepConfig.UpdateSize size = new DbToDbStepConfig.UpdateSize();
        size.setSize(2);
        size.setBean(EtlMergeInputWorkEntity.class);
        stepConfig.setUpdateSize(size);
        stepConfig.initialize();
        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        try {
            sut.process();
            fail();
        } catch (SqlStatementException e) {
        }
        connection.commit();

        // -------------------------------------------------- assert
        final List<EtlMergeEntity> result = VariousDbTestHelper.findAll(EtlMergeEntity.class, "userId");
        assertThat("1回目のSQL実行は成功し、2回目のSQL実行に失敗するので元の1レコード＋成功した2レコードの3レコードのみ",
                result.size(), is(3));
        assertThat(result.get(0).userId, is(1L));
        assertThat(result.get(0).name, is("name1"));
        assertThat(result.get(0).address, is("address1"));
        assertThat(result.get(1).userId, is(2L));
        assertThat(result.get(1).name, is("name2"));
        assertThat(result.get(1).address, is("address2"));
        assertThat(result.get(2).userId, is(3L));
        assertThat(result.get(2).name, is("3"));
        assertThat(result.get(2).address, is("3"));

        final List<String> messages = OnMemoryLogWriter.getMessages("writer.progress");
        assertThat(messages, Matchers.contains(
                containsString("input count: [5]"),
                containsString("remaining count: [3]")
        ));
    }

    /**
     * 1回のSQL実行で処理するサイズが指定されたがSQL文にINパラメータがない場合に、例外が送出されること。
     */
    @Test
    public void invalidSqlUsingSplit() throws Exception {

        // -------------------------------------------------- setup objects that is injected
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(EtlMergeEntity.class);
        stepConfig.setMergeOnColumns(Arrays.asList("user_id", "name"));
        stepConfig.setSqlId("SELECT_ALL");
        final DbToDbStepConfig.UpdateSize size = new DbToDbStepConfig.UpdateSize();
        size.setSize(2);
        size.setBean(EtlMergeInputWorkEntity.class);
        stepConfig.setUpdateSize(size);
        stepConfig.initialize();
        final MergeBatchlet sut = new MergeBatchlet(
                mockJobContext,
                mockStepContext,
                stepConfig,
                new RangeUpdateHelper(mockJobContext, mockStepContext),
                new BasicProgressManager(mockJobContext, mockStepContext)
        );

        // -------------------------------------------------- execute
        expectedException.expect(InvalidEtlConfigException.class);
        expectedException.expectMessage("sql is invalid. "
                + "please include a range of data on the condition of the sql statement using the two input parameters. "
                + "ex) \"where line_number between ? and ?\" "
                + "sqlId = [SELECT_ALL]");
        sut.process();
    }
    
    private static final String COMMIT_MSG = "transaction commit.";

    /**
     * SQL実行とコミットの回数を検証する。
     * @param expectedMessages 「update count = [n]」と「transaction commit.」メッセージ
     */
    private void assertSqlExecutionAndCommitTimes(final String... expectedMessages) {

        // 実際に出力された順番で、SQL実行とコミットのメッセージを抜き出します。
        final List<String> actualMessages = new ArrayList<String>();
        for (String message : OnMemoryLogWriter.getMessages("writer.sql")) {
            if (message.contains("update count = ")
                    || message.contains(COMMIT_MSG)) {
                actualMessages.add(message);
            }
        }

        // 実際に出力された順番のメッセージと、期待するメッセージをアサートします。
        assertThat(actualMessages.size(), is(expectedMessages.length));
        for (int i = 0; i < expectedMessages.length; i++) {
            assertThat(actualMessages.get(i), is(containsString(expectedMessages[i])));
        }
    }

    @Entity
    @Table(name = "etl_merge_input_work_entity")
    public static class EtlMergeInputWorkEntity {

        @Id
        @Column(name = "work_user_id", length = 15)
        public Long userId;

        @Column(name = "work_name")
        public String name;

        @Column(name = "work_address")
        public String address;

        @Column(name = "LINE_NUMBER", length = 18, nullable = false, unique = true)
        public Long lineNumber;

        public EtlMergeInputWorkEntity() {
        }

        public EtlMergeInputWorkEntity(Long lineNumber, Long userId, String name, String address) {
            this.userId = userId;
            this.name = name;
            this.address = address;
            this.lineNumber = lineNumber;
        }
    }

    @Entity
    @Table(name = "etl_merge_entity")
    public static class EtlMergeEntity {

        @Id
        @Column(name = "user_id", length = 15)
        public Long userId;

        @Column(name = "name", length = 20)
        public String name;

        @Column(name = "address")
        public String address;

        public EtlMergeEntity() {
        }

        public EtlMergeEntity(Long userId, String name, String address) {
            this.userId = userId;
            this.name = name;
            this.address = address;
        }

        @Id
        public Long getUserId() {
            return userId;
        }

        public String getName() {
            return name;
        }

        public String getAddress() {
            return address;
        }
    }

    @Entity
    @Table(name = "etl_merge_multikey")
    public static class EtlMergeMultiKey {

        @Id
        @Column(name = "id", length = 10)
        public Long id;

        @Id
        @Column(name = "mail_address")
        public String mailAddress;

        @Column(name = "name")
        public String name;

        public EtlMergeMultiKey() {
        }

        public EtlMergeMultiKey(final Long id, final String mailAddress, final String name) {
            this.id = id;
            this.mailAddress = mailAddress;
            this.name = name;
        }

        @Id
        public Long getId() {
            return id;
        }

        @Id
        public String getMailAddress() {
            return mailAddress;
        }

        public String getName() {
            return name;
        }
    }

    @Entity
    @Table(name = "etl_merge_multi_key_work")
    public static class EtlMergeMultiKeyWork {

        @Id
        @Column(name = "line_number", length = 10)
        public Long lineNumber;

        @Column(name = "id")
        public String id;

        @Column(name = "mail_address")
        public String mailAddress;

        @Column(name = "name")
        public String name;

        public EtlMergeMultiKeyWork() {
        }

        public EtlMergeMultiKeyWork(final Long lineNumber, final String id, final String mailAddress,
                final String name) {
            this.lineNumber = lineNumber;
            this.id = id;
            this.mailAddress = mailAddress;
            this.name = name;
        }
    }
}

