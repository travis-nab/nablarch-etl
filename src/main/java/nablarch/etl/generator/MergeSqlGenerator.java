package nablarch.etl.generator;

import java.util.List;

import nablarch.common.dao.ColumnMeta;
import nablarch.common.dao.EntityUtil;
import nablarch.etl.config.DbToDbStepConfig;

/**
 * 入力リソース(SELECT)文から登録/更新を一括で行うMERGE文を生成するクラス。
 *
 * @author Hisaaki Shioiri
 */
public class MergeSqlGenerator extends SqlGeneratorSupport<DbToDbStepConfig> {

    /** 入力テーブルの別名 */
    private static final String INPUT_TABLE_ALIAS = "input_";

    /** 出力テーブルの別名 */
    private static final String OUTPUT_TABLE_ALIAS = "output_";

    @Override
    public String generateSql(final DbToDbStepConfig config) {

        Class<?> clazz = config.getBean();
        verify(clazz);

        final String tableName = EntityUtil.getTableName(clazz);
        final StringBuilder mergeSql = new StringBuilder(512);

        final List<String> joinColumns = config.getMergeOnColumns();
        mergeSql.append("merge into ")
                .append(tableName)
                .append(' ')
                .append(OUTPUT_TABLE_ALIAS)
                .append(" using (")
                .append(config.getSql())
                .append(") ")
                .append(INPUT_TABLE_ALIAS)
                .append(" on (")
                .append(makeOnClause(joinColumns))
                .append(')')
                .append(" when matched then")
                .append(" update set ")
                .append(makeSetClause(clazz, joinColumns))
                .append(" when not matched then insert (")
                .append(makeInsertClause(clazz));


        return mergeSql.toString();
    }

    /**
     * MERGE文のON句を構築する。
     *
     * @param joinColumns 結合カラム
     * @return 構築したON句
     */
    private static String makeOnClause(final List<String> joinColumns) {
        final StringBuilder result = new StringBuilder(128);
        for (int i = 0; i < joinColumns.size(); i++) {
            if (i != 0) {
                result.append(" and ");
            }
            final String columnName = joinColumns.get(i);
            result.append(OUTPUT_TABLE_ALIAS)
                    .append('.')
                    .append(columnName)
                    .append(" = ")
                    .append(INPUT_TABLE_ALIAS)
                    .append('.')
                    .append(columnName);
        }
        return result.toString();
    }

    /**
     * INSERT文を構築する。
     *
     * @param clazz Entityクラス
     * @return 構築したINSERT文
     */
    private static String makeInsertClause(final Class<?> clazz) {
        final List<ColumnMeta> columns = EntityUtil.findAllColumns(clazz);

        final StringBuilder insert = new StringBuilder(256);
        final StringBuilder values = new StringBuilder(256);
        for (int i = 0; i < columns.size(); i++) {
            if (i != 0) {
                insert.append(',');
                values.append(',');
            }
            final String columnName = columns.get(i)
                    .getName();
            insert.append(columnName);

            values.append(INPUT_TABLE_ALIAS)
                    .append('.')
                    .append(columnName);
        }
        insert.append(") values (")
                .append(values)
                .append(')');
        return insert.toString();
    }

    /**
     * UPDATEのSET句を構築する。
     *
     * @param clazz Entityクラス
     * @param joinColumns 結合カラム
     * @return 構築したSET句
     */
    private static String makeSetClause(final Class<?> clazz, final List<String> joinColumns) {
        final List<ColumnMeta> columns = EntityUtil.findAllColumns(clazz);
        final List<ColumnMeta> keys = EntityUtil.findIdColumns(clazz);

        final StringBuilder result = new StringBuilder(256);
        for (ColumnMeta column : columns) {
            if (keys.contains(column) || isJoinColumn(joinColumns, column)) {
                continue;
            }
            if (result.length() != 0) {
                result.append(", ");
            }
            result.append(OUTPUT_TABLE_ALIAS)
                    .append('.')
                    .append(column.getName())
                    .append(" = ")
                    .append(INPUT_TABLE_ALIAS)
                    .append('.')
                    .append(column.getName());
        }
        return result.toString();
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

