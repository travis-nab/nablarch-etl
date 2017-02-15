package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import nablarch.common.dao.ColumnMeta;
import nablarch.common.dao.EntityUtil;
import nablarch.core.db.connection.DbConnectionContext;
import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.repository.SystemRepository;
import nablarch.etl.InvalidEtlConfigException;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.test.support.SystemRepositoryResource;
import nablarch.test.support.db.helper.DatabaseTestRunner;
import nablarch.test.support.db.helper.VariousDbTestHelper;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import mockit.Mocked;
import mockit.NonStrictExpectations;

/**
 * {@link MergeSqlGenerator}のテスト。
 */
@RunWith(DatabaseTestRunner.class)
public class MergeSqlGeneratorTest {

    SqlGeneratorSupport sut = new MergeSqlGenerator();

    @ClassRule
    public static SystemRepositoryResource resource = new SystemRepositoryResource("db-default.xml");

    @BeforeClass
    public static void setUpClass() throws Exception {
        VariousDbTestHelper.createTable(EtlMergeGenEntity.class);
    }

    @Mocked
    TransactionManagerConnection mockConnection;

    Connection connection;

    @Before
    public void setUp() throws Exception {
        connection = VariousDbTestHelper.getNativeConnection();
        new NonStrictExpectations() {{
            mockConnection.getConnection();
            result = connection;
        }};
        DbConnectionContext.setConnection(mockConnection);
    }

    @After
    public void tearDown() throws Exception {
        connection.close();
        DbConnectionContext.removeConnection();
        SystemRepository.clear();
    }

    /**
     * MERGE文が自動生成されること
     */
    @Test
    public void generateMergeSql() throws Exception {

        final String inputResource = "select id test_id, name1 last_name, name2 first_name from input_table";

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setBean(EtlMergeGenEntity.class);
                setMergeOnColumns(Arrays.asList("test_id"));
            }
            @Override
            public String getSql() {
                return inputResource;
            }
        };

        final String actual = sut.generateSql(config);

        assertThat("MERGE文が生成されること", actual,
                is("merge into etl_merge_gen output_"
                                + " using (" + inputResource + ") input_"
                                + " on (output_.test_id = input_.test_id)"
                                + " when matched then update set " + makeSetClause(EtlMergeGenEntity.class)
                                + " when not matched then insert (" + makeInsertClause(EtlMergeGenEntity.class, "")
                                + ") values (" + makeInsertClause(EtlMergeGenEntity.class, "input_") + ')'
                ));

    }

    /**
     * 複数の結合カラムが設定されている場合でも、MERGE文が正しく自動生成されること。
     */
    @Test
    public void generateMergeSql_multiJoinColumn() throws Exception {

        final String inputResource = "select id test_id, name1 last_name, name2 first_name from input_table";

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setBean(EtlMergeGenEntity.class);
                setMergeOnColumns(Arrays.asList("first_name", "last_name"));
            }
            @Override
            public String getSql() {
                return inputResource;
            }
        };

        final String actual = sut.generateSql(config);

        assertThat("MERGE文が生成されること", actual,
                is("merge into etl_merge_gen output_"
                                + " using (" + inputResource + ") input_"
                                // 結合カラムが複数の場合andで連結される
                                + " on (output_.first_name = input_.first_name and output_.last_name = input_.last_name)"
                                + " when matched then update set " + makeSetClause(EtlMergeGenEntity.class, "first_name", "last_name")
                                + " when not matched then insert (" + makeInsertClause(EtlMergeGenEntity.class, "")
                                + ") values (" + makeInsertClause(EtlMergeGenEntity.class, "input_") + ')'
                ));
    }

    /**
     * Entity以外のクラスを指定した場合、エラーとなること。
     */
    @Test(expected = InvalidEtlConfigException.class)
    public void notEntityClass() throws Exception {

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setBean(ArrayList.class);
            }
            @Override
            public String getSql() {
                return "";
            }
        };

        sut.generateSql(config);
    }

    /**
     * INSERT用の項目を列挙する。
     *
     * @param clazz Entityクラス
     * @param prefix プレフィックス
     * @return 結果
     */
    private String makeInsertClause(Class<?> clazz, String prefix) {
        final List<ColumnMeta> columns = EntityUtil.findAllColumns(clazz);
        StringBuilder result = new StringBuilder(512);
        for (ColumnMeta column : columns) {
            if (result.length() != 0) {
                result.append(',');
            }
            if (!prefix.isEmpty()) {
                result.append(prefix)
                        .append('.');
            }
            result.append(column.getName());
        }
        return result.toString();
    }

    /**
     * KEY項目は除外してSET句を構築する。
     *
     * @param clazz Entityクラス
     * @param joinColumns 結合カラム
     * @return SET句
     */
    private String makeSetClause(Class<?> clazz, String... joinColumns) {
        final List<ColumnMeta> columns = EntityUtil.findAllColumns(clazz);
        final List<ColumnMeta> keys = EntityUtil.findIdColumns(clazz);

        final StringBuilder result = new StringBuilder();
        for (ColumnMeta column : columns) {
            if (keys.contains(column)) {
                continue;
            }
            boolean isJoinColumn = false;
            for (String joinColumn : joinColumns) {
                if (joinColumn.equalsIgnoreCase(column.getName())) {
                    isJoinColumn = true;
                    break;
                }
            }
            if (isJoinColumn) {
                continue;
            }
            if (result.length() != 0) {
                result.append(", ");
            }
            result.append("output_.")
                    .append(column.getName())
                    .append(" = input_.")
                    .append(column.getName());
        }
        return result.toString();
    }

    @Entity
    @Table(name = "etl_merge_gen")
    public static class EtlMergeGenEntity {

        @Id
        @Column(name = "TEST_ID", length = 15)
        public Long id;

        @Column(name = "last_name")
        public String lastName;

        @Column(name = "first_name")
        public String firstName;

        @Id
        @Column(name = "TEST_ID")
        public Long getId() {
            return id;
        }

        public String getLastName() {
            return lastName;
        }

        public String getFirstName() {
            return firstName;
        }
    }

}

