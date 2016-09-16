package nablarch.etl.generator;

import nablarch.common.dao.EntityUtil;
import nablarch.etl.config.DbToDbStepConfig;

/**
 * LINE_NUMBERカラムの最大値を取得するSQL文を生成するクラス。
 * @author Kiyohito Itoh
 */
public class MaxLineNumberSqlGenerator extends SqlGeneratorSupport<DbToDbStepConfig> {

    @Override
    public String generateSql(DbToDbStepConfig config) {

        final Class<?> clazz = config.getUpdateSize().getBean();
        verify(clazz);

        final String tableName = EntityUtil.getTableNameWithSchema(clazz);

        return "select max(LINE_NUMBER) from " + tableName;
    }
}
