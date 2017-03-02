package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.persistence.Entity;
import javax.persistence.Table;

import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.core.transaction.TransactionFactory;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link Db2TruncateSqlGenerator}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class Db2TruncateSqlGeneratorTest {

    @ClassRule
    public static SystemRepositoryResource resource = new SystemRepositoryResource("db-default.xml");

    @Before
    public void setUp() throws Exception {
        ConnectionFactory connectionFactory = resource.getComponent("connectionFactory");
        DbConnectionContext.setConnection(connectionFactory.getConnection(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));

        TransactionFactory transactionFactory = resource.getComponent("jdbcTransactionFactory");
        TransactionContext.setTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY,
                transactionFactory.getTransaction(TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY));
    }

    @After
    public void tearDown() throws Exception {
        TransactionContext.removeTransaction();
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection();
        DbConnectionContext.removeConnection();
        connection.terminate();
    }

    /** テスト対象 */
    TruncateSqlGenerator sut = new Db2TruncateSqlGenerator();

    @Test
    public void testGenerateSql_tableOnly() throws Exception {
        assertThat(sut.generateSql(TableOnly.class), is("truncate table tableOnly immediate"));
    }

    @Test
    public void testGenerateSql_schemaOnly() throws Exception {
        assertThat(sut.generateSql(SchemaOnly.class), is("truncate table test.SCHEMA_ONLY immediate"));
    }

    @Test
    public void testGenerateSql_nothing() throws Exception {
        assertThat(sut.generateSql(Nothing.class), is("truncate table NOTHING immediate"));
    }

    @Test
    public void testGenerateSql_tableAndSchema() throws Exception {
        assertThat(sut.generateSql(TableAndSchema.class), is("truncate table test.tableAndSchema immediate"));
    }

    @Entity
    @Table(name = "tableOnly")
    private static class TableOnly {}

    @Entity
    @Table(schema = "test")
    private static class SchemaOnly {}

    @Entity
    private static class Nothing {}

    @Entity
    @Table(name = "tableAndSchema", schema = "test")
    private static class TableAndSchema {}

}