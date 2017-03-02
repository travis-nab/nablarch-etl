package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.persistence.Table;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link TruncateSqlGenerator}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class TruncateSqlGeneratorTest {

    @ClassRule
    public static SystemRepositoryResource resource = new SystemRepositoryResource("db-default.xml");

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = resource.getComponent("connectionFactory");
        DbConnectionContext.setConnection(connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
    }

    @After
    public void tearDown() throws Exception {
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection();
        DbConnectionContext.removeConnection();
        connection.terminate();
    }

    /** テスト対象 */
    TruncateSqlGenerator sut = new TruncateSqlGenerator();

    @Test
    public void testGenerateSql() throws Exception {
        assertThat(sut.generateSql(TruncateEntity.class), is("truncate table test.truncate_table"));
    }

    @Table(name = "truncate_table", schema = "test")
    private static class TruncateEntity {
        public Long id;
        public String name;
    }

}