package nablarch.etl.generator;

import nablarch.common.dao.EntityUtil;

/**
 * TRUNCATE文を構築するクラス。
 *
 * @author Naoki Yamamoto
 */
public class TruncateSqlGenerator {

    /**
     * TRUNCATE文を構築する。
     *
     * @param entity エンティティクラス
     * @return TRUNCATE文
     */
    public String generateSql(final Class<?> entity) {
        return "truncate table " + EntityUtil.getTableNameWithSchema(entity);
    }
}
