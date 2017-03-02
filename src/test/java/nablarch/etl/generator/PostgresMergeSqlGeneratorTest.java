package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.common.dao.ColumnMeta;
import nablarch.common.dao.EntityUtil;
import nablarch.core.db.connection.ConnectionFactory;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.transaction.TransactionContext;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link PostgresMergeSqlGenerator}のテスト。
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(include = TargetDb.Db.POSTGRE_SQL)
public class PostgresMergeSqlGeneratorTest {

    @Rule
    public SystemRepositoryResource repositoryResource = new SystemRepositoryResource("db-default.xml");

    private final PostgresMergeSqlGenerator sut = new PostgresMergeSqlGenerator();

    @Before
    public void setUp() throws Exception {
        final ConnectionFactory connectionFactory = repositoryResource.getComponentByType(ConnectionFactory.class);
        final TransactionManagerConnection connection = connectionFactory.getConnection(
                TransactionContext.DEFAULT_TRANSACTION_CONTEXT_KEY);
        DbConnectionContext.setConnection(connection);

        VariousDbTestHelper.createTable(PostgresEntity.class);
    }

    @After
    public void tearDown() throws Exception {
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection();
        connection.terminate();
        DbConnectionContext.removeConnection();
    }

    @Test
    public void testSingleMergeColumn() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(PostgresEntity.class);
        stepConfig.setSqlId("select_all");
        stepConfig.setMergeOnColumns(Collections.singletonList("id"));
        stepConfig.initialize();

        final String sql = sut.generate(stepConfig);
        assertThat(sql, is("insert into postgres_entity(id,name,address)" 
                + " select id, name, address from input_table"
                + " on conflict(id) do update set "
                + createUpdateColumns(PostgresEntity.class, Collections.singletonList("ID"))
        ));
    }

    @Test
    public void testMultiMergeColumn() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(PostgresEntity.class);
        stepConfig.setSqlId("select_all");
        stepConfig.setMergeOnColumns(Arrays.asList("id", "name"));
        stepConfig.initialize();

        final String sql = sut.generate(stepConfig);
        assertThat(sql, is("insert into postgres_entity(id,name,address)"
                + " select id, name, address from input_table"
                + " on conflict(id,name) do update set "
                + createUpdateColumns(PostgresEntity.class, Arrays.asList("ID", "NAME"))
        ));
    }

    private String createUpdateColumns(Class<?> entity, List<String> excludeColumns) {
        final List<ColumnMeta> columns = EntityUtil.findAllColumns(entity);
        final StringBuilder sql = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            final ColumnMeta column = columns.get(i);
            final String columnName = column.getName();
            if (excludeColumns.contains(columnName)) {
                continue;
            }

            if (sql.length() != 0) {
                sql.append(',');
            }
            sql.append(columnName);
            sql.append("=excluded.");
            sql.append(columnName);
        }
        return sql.toString();
    }

    @Entity
    @Table(name = "postgres_entity")
    public static class PostgresEntity {

        @Id
        @Column(name = "id")
        public Long id;

        @Column(name = "name")
        public String name;

        @Column(name = "address")
        public String address;

        @Id
        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(final String address) {
            this.address = address;
        }
    }
}