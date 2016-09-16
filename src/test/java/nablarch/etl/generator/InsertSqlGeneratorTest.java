package nablarch.etl.generator;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Map;

import nablarch.core.util.StringUtil;
import nablarch.etl.config.DbToDbStepConfig;
import nablarch.test.support.db.helper.DatabaseTestRunner;

import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * {@link InsertSqlGenerator}のテストクラス。
 */
@RunWith(DatabaseTestRunner.class)
public class InsertSqlGeneratorTest extends InsertSqlGeneratorTestSupport {

    SqlGeneratorSupport sut = new InsertSqlGenerator();

    /**
     * INSERT INTO～SELECTなSQL文が生成できること
     */
    @Test
    public void generateInsertSql() throws Exception {

        final String inputResource = "select id, name1, name2 from input_table";

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setBean(EtlInsertGenEntity.class);
            }
            @Override
            public String getSql() {
                return inputResource;
            }
        };

        final String result = sut.generateSql(config);

        assertThat("insert into ～ selectが生成されること", result,
                is("insert into etl_insert_gen("
                        + StringUtil.join(",", getColumnNames("etl_insert_gen"))
                        + ") " + inputResource));
    }

    /**
     * 非Entityクラスはエラーとなること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void notEntityClass() throws Exception {

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setBean(Map.class);
            }
            @Override
            public String getSql() {
                return "";
            }
        };

        sut.generateSql(config);
    }

}

