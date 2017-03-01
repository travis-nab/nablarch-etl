package nablarch.etl.generator;

import java.util.List;

import nablarch.common.dao.ColumnMeta;
import nablarch.common.dao.EntityUtil;
import nablarch.core.util.StringUtil;
import nablarch.etl.EtlUtil;
import nablarch.etl.config.DbToDbStepConfig;

/**
 * PostgreSQLでは、MERGE文がサポートされていないため代替機能のUPSERTを生成するクラス。
 *
 * @author siosio
 */
public class PostgresMergeSqlGenerator extends MergeSqlGenerator {

    @Override
    public String generate(final DbToDbStepConfig config) {
        final Class<?> entityClass = config.getBean();
        final List<String> mergeOnColumns = config.getMergeOnColumns();
        final String tableName = EntityUtil.getTableName(entityClass);

        final StringBuilder sql = new StringBuilder(512);
        sql.append("insert into ");
        sql.append(tableName);
        sql.append('(');
        sql.append(makeInsertColumnNames(tableName));
        sql.append(") ");
        sql.append(config.getSql());
        sql.append(" on conflict(");
        sql.append(StringUtil.join(",", mergeOnColumns));
        sql.append(") do update set ");
        sql.append(makeUpdateSql(entityClass, mergeOnColumns));
        return sql.toString();
    }

    /**
     * insert句のカラムリストを生成する。
     *
     * @param tableName テーブル名
     * @return insert句のカラムリスト
     */
    private static String makeInsertColumnNames(final String tableName) {
        final List<String> columns = EtlUtil.getAllColumns(tableName);

        final StringBuilder sql = new StringBuilder(256);
        final int size = columns.size();
        for (int i = 0; i < size; i++) {
            if (i != 0) {
                sql.append(',');
            }
            sql.append(columns.get(i));
        }
        return sql.toString();
    }

    /**
     * updateのset句のsqlを生成する。
     *
     * @param entityClass 対象のEntityClass
     * @param mergeOnColumns MERGEのON句に指定するカラムリスト(ON CONSTRAINTに指定するカラムリスト)
     * @return sql
     */
    private static String makeUpdateSql(final Class<?> entityClass, final List<String> mergeOnColumns) {
        final List<ColumnMeta> columns = EntityUtil.findAllColumns(entityClass);
        final List<ColumnMeta> keys = EntityUtil.findIdColumns(entityClass);
        final StringBuilder sql = new StringBuilder(256);

        final int size = columns.size();
        for (final ColumnMeta column : columns) {
            if (keys.contains(column) || isJoinColumn(mergeOnColumns, column)) {
                continue;
            }

            if (sql.length() != 0) {
                sql.append(',');
            }
            final String columnName = column.getName();
            sql.append(columnName);
            sql.append("=excluded.");
            sql.append(columnName);
        }
        return sql.toString();
    }

    /**
     * 結合カラムか否か。
     *
     * @param joinColumns 結合カラムリスト
     * @param column カラム
     * @return 結合カラムの場合{@code true}
     */
    private static boolean isJoinColumn(final List<String> joinColumns, final ColumnMeta column) {
        for (String joinColumn : joinColumns) {
            if (joinColumn.equalsIgnoreCase(column.getName())) {
                return true;
            }
        }
        return false;
    }

}
