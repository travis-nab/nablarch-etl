package nablarch.etl.generator;

import nablarch.etl.config.DbToDbStepConfig;
import org.junit.Test;

import javax.persistence.Entity;
import javax.persistence.Table;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * {@link MaxLineNumberSqlGenerator}のテスト。
 */
public class MaxLineNumberSqlGeneratorTest {

    private final MaxLineNumberSqlGenerator sut = new MaxLineNumberSqlGenerator();

    /**
     * 設定で指定されたEntityのテーブル名が入ったSQL文が生成されること。
     */
    @Test
    public void generateMaxLineNumberSql() {

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setUpdateSize(new UpdateSize());
                getUpdateSize().setBean(EtlMaxLineNumberGenEntity.class);
            }
        };

        final String result = sut.generateSql(config);

        assertThat(result, is("select max(LINE_NUMBER) from etl_work.etl_max_line_number_gen"));
    }

    /**
     * 非Entityクラスはエラーとなること。
     */
    @Test(expected = IllegalArgumentException.class)
    public void notEntityClass() throws Exception {

        final DbToDbStepConfig config = new DbToDbStepConfig() {
            {
                setUpdateSize(new UpdateSize());
                getUpdateSize().setBean(Map.class);
            }
        };

        sut.generateSql(config);
    }

    @Entity
    @Table(name = "etl_max_line_number_gen", schema = "etl_work")
    public static class EtlMaxLineNumberGenEntity {
    }
}
