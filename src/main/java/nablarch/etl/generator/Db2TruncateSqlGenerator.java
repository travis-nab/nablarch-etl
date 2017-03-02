package nablarch.etl.generator;

import nablarch.common.dao.EntityUtil;

/**
 * DB2用のTRUNCATE文を構築するクラス。
 *
 * @author Naoki Yamamoto
 */
public class Db2TruncateSqlGenerator extends TruncateSqlGenerator {

    @Override
    public String generateSql(final Class<?> entity) {
        return "truncate table " + EntityUtil.getTableNameWithSchema(entity) + " immediate";
    }
}
