package nablarch.etl.generator;

import java.util.List;

import nablarch.common.dao.EntityUtil;
import nablarch.core.util.StringUtil;
import nablarch.etl.EtlUtil;
import nablarch.etl.config.DbToDbStepConfig;

/**
 * 入力リソース(SELECT)文から登録/更新を一括で行うH2データベース用のMERGE文を生成するクラス。
 *
 * @author Hisaaki Shioiri
 */
public class H2MergeSqlGenerator extends MergeSqlGenerator {

    @Override
    public String generate(final DbToDbStepConfig config) {

        final Class<?> entityClass = config.getBean();
        final String tableName = EntityUtil.getTableNameWithSchema(entityClass);
        final String selectSql = config.getSql();
        final StringBuilder mergeSql = new StringBuilder(512);

        final List<String> joinColumns = config.getMergeOnColumns();
        mergeSql.append("merge into ")
                .append(tableName)
                .append('(')
                .append(makeIntoColumnClause(entityClass))
                .append(") key(")
                .append(StringUtil.join(",", joinColumns))
                .append(") ")
                .append(selectSql);
        return mergeSql.toString();
    }

    /**
     * INTO句のカラム名リストを構築する。
     *
     * @param entityClass エンティティクラス
     * @return INTO句のカラム名リスト
     */
    private static String makeIntoColumnClause(final Class<?> entityClass) {
        final List<String> columns = EtlUtil.getAllColumns(EntityUtil.getTableName(entityClass));

        final StringBuilder sql = new StringBuilder(256);
        final int size = columns.size();
        for (int i = 0; i < size; i++) {
            final String column = columns.get(i);
            if (i != 0) {
                sql.append(',');
            }
            sql.append(column);
        }
        return sql.toString();
    }
}

