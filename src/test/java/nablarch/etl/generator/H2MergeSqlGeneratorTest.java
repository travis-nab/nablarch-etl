package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.core.db.connection.BasicDbConnection;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.TargetDb;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link H2MergeSqlGenerator}のテスト。
 */
@RunWith(DatabaseTestRunner.class)
@TargetDb(include = TargetDb.Db.H2)
public class H2MergeSqlGeneratorTest {

    @ClassRule
    public static SystemRepositoryResource resource = new SystemRepositoryResource("db-default.xml");

    private MergeSqlGenerator sut = new H2MergeSqlGenerator();
    
    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(H2EntityWithSchema.class);
        VariousDbTestHelper.createTable(H2Entity1.class);
        VariousDbTestHelper.createTable(H2Entity2.class);
    }

    @Before
    public void setUp() throws Exception {
        Connection connection = VariousDbTestHelper.getNativeConnection();
        DbConnectionContext.setConnection(new BasicDbConnection(connection));
    }

    @After
    public void tearDown() throws Exception {
        final TransactionManagerConnection connection = DbConnectionContext.getTransactionManagerConnection();
        DbConnectionContext.removeConnection();
        connection.rollback();
    }

    /**
     * key項目が1つの場合でmerge文の生成確認
     */
    @Test
    public void generateSqlSingleKeyColumn() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(H2Entity1.class);
        stepConfig.setSqlId("select_input");
        stepConfig.setMergeOnColumns(Arrays.asList("id"));
        stepConfig.initialize();
        final String sql = sut.generateSql(stepConfig);

        assertThat(sql, is("merge into h2_entity1(ID,NAME,MAIL_ADDRESS)"
                + " key(id) select id, name, mail_address from input_table"));
    }

    /**
     * key項目が複数の場合でmerge文の生成確認
     */
    @Test
    public void generateSqlMultiKeyColumn() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(H2Entity2.class);
        stepConfig.setSqlId("select_input");
        stepConfig.setMergeOnColumns(Arrays.asList("id", "id2"));
        stepConfig.initialize();
        final String sql = sut.generateSql(stepConfig);

        assertThat(sql, is("merge into h2_entity2(ID,ID2,NAME,MAIL_ADDRESS)"
                + " key(id,id2) select id, id2, name, mail_address from input_table"));
    }

    /**
     * スキーマ指定ありのエンティティでmerge文の生成確認
     * @throws Exception
     */
    @Test
    public void generateSqlWithSchema() throws Exception {
        final DbToDbStepConfig stepConfig = new DbToDbStepConfig();
        stepConfig.setBean(H2EntityWithSchema.class);
        stepConfig.setSqlId("select");
        stepConfig.setMergeOnColumns(Collections.singletonList("id"));
        stepConfig.initialize();
        final String sql = sut.generateSql(stepConfig);

        assertThat(sql, is("merge into ssd_master.h2_entity_with_schema(ID,NAME)"
                + " key(id) select id, name, from input_table"));
    }

    @Entity
    @Table(name = "h2_entity1")
    public static class H2Entity1 {

        @Id
        @Column(name = "id")
        public Long id;

        @Column(name = "name")
        public String name;

        @Column(name = "mail_address")
        public String mailAddress;

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

        public String getMailAddress() {
            return mailAddress;
        }

        public void setMailAddress(final String mailAddress) {
            this.mailAddress = mailAddress;
        }
    }

    @Entity
    @Table(name = "h2_entity2")
    public static class H2Entity2 {

        @Id
        @Column(name = "id")
        public Long id;

        @Id
        @Column(name = "id2")
        public String id2;

        @Column(name = "name")
        public String name;

        @Column(name = "mail_address")
        public String mailAddress;

        public Long getId() {
            return id;
        }

        public void setId(final Long id) {
            this.id = id;
        }

        public String getId2() {
            return id2;
        }

        public void setId2(final String id2) {
            this.id2 = id2;
        }

        public String getName() {
            return name;
        }

        public void setName(final String name) {
            this.name = name;
        }

        public String getMailAddress() {
            return mailAddress;
        }

        public void setMailAddress(final String mailAddress) {
            this.mailAddress = mailAddress;
        }
    }
    
    @Entity
    @Table(name = "h2_entity_with_schema", schema = "ssd_master")
    public static class H2EntityWithSchema {

        @Id
        @Column(name = "id")
        public Long id;

        @Column(name = "name")
        public String name;

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
    }
}