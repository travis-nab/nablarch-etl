package nablarch.etl.generator;

import javax.persistence.Entity;

import nablarch.common.dao.EntityUtil;
import nablarch.core.util.StringUtil;
import nablarch.core.util.annotation.Published;
import nablarch.etl.EtlUtil;
import nablarch.etl.config.DbToDbStepConfig;

/**
 * 一括登録用のINSERT文を生成するクラス。
 * <p/>
 * このクラスは、与えられたSELECT文の結果を一括で登録するINSERT文を生成する。
 * 登録対象のテーブルは指定されたEntityクラス({@link Entity}アノテーションが設定されたクラス)に設定されたテーブルとなる。
 * <p/>
 * SELECT文のSELECT句の順序は、登録対象のテーブルのカラム順と一致している必要がある。
 * <p/>
 * 以下に生成されるSQL文の例を示す。
 * <pre>
 * {@code
 *
 * 登録対象のテーブル:user
 * SELECT文:select id, name from user_work
 *
 * 生成されるSQL文:insert into user (id, name) select id, name from user_work
 * }
 * </pre>
 *
 * @author Hisaaki Shioiri
 * @see DbToDbStepConfig
 */
@Published(tag = "architect")
public class InsertSqlGenerator extends SqlGeneratorSupport<DbToDbStepConfig> {

    /**
     * 一括登録用のINSERT文を生成する。
     *
     * @param config ステップの設定
     * @return 生成したINSERT文
     */
    @Override
    public String generateSql(final DbToDbStepConfig config) {

        final Class<?> clazz = config.getBean();
        verify(clazz);

        final StringBuilder insertSql = new StringBuilder(512);

        insertSql.append(generateInsertIntoClause(config))
                .append(' ')
                .append(generateInsertTableName(config))
                .append(generateInsertColumnList(config))
                .append(generateSourceSql(config));

        return insertSql.toString();
    }

    /**
     * {@code insert into}句を生成する。
     *
     * @param config ステップの設定(この実装では使用しない)
     * @return {@code insert into}句
     */
    protected String generateInsertIntoClause(final DbToDbStepConfig config) {
        return "insert into";
    }

    /**
     * insert対象のテーブル名を生成する。
     *
     * @param config ステップの設定
     * @return テーブル名
     * @see DbToDbStepConfig#getBean()
     */
    protected String generateInsertTableName(final DbToDbStepConfig config) {
        final Class<?> clazz = config.getBean();
        return EntityUtil.getTableNameWithSchema(clazz);
    }

    /**
     * insert対象のカラムリスト(括弧つき)を生成する。
     *
     * @param config ステップの設定
     * @return insert対象のカラムリスト(括弧つき)
     * @see DbToDbStepConfig#getBean()
     */
    protected String generateInsertColumnList(final DbToDbStepConfig config) {
        final StringBuilder columnList = new StringBuilder(256);

        final Class<?> clazz = config.getBean();
        final String tableName = EntityUtil.getTableName(clazz);
        columnList.append('(')
                .append(StringUtil.join(",", EtlUtil.getAllColumns(tableName)))
                .append(") ");
        return columnList.toString();
    }

    /**
     * insert対象のデータを取得するselect文を生成する。
     *
     * @param config ステップの設定
     * @return insert対象のデータを取得するselect文
     * @see DbToDbStepConfig#getSql()
     */
    protected String generateSourceSql(final DbToDbStepConfig config) {
        return config.getSql();
    }
}

