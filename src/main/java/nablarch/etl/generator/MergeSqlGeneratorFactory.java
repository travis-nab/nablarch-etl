package nablarch.etl.generator;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.etl.EtlUtil;

/**
 * MERGE文のジェネレータのファクトリクラス。
 * <p>
 * {@link DatabaseMetaData#getURL()}を元に、接続さきデータベース製品を判断し、MERGE文のジェネレータを生成する。
 * <p>
 * MERGE文に対応するデータベースは以下の通り。
 * <ul>
 * <li>Oracle</li>
 * <li>H2</li>
 * <li>SQL Server</li>
 * <li>PostgreSQL</li>
 * <li>DB2</li>
 * </ul>
 *
 * @author siosio
 */
public final class MergeSqlGeneratorFactory {

    /**
     * 隠蔽コンストラクタ。
     */
    private MergeSqlGeneratorFactory() {
    }

    /**
     * MERGE文を生成するジェネレータを生成する。
     *
     * @param connection データベース接続
     * @return MERGE文のジェネレータ
     */
    public static MergeSqlGenerator create(final TransactionManagerConnection connection) {
        final String url = EtlUtil.getUrl(connection);
        if (url.startsWith("jdbc:oracle") || url.startsWith("jdbc:db2")) {
            return new StandardMergeSqlGenerator();
        } else if (url.startsWith("jdbc:h2")) {
            return new H2MergeSqlGenerator();
        } else if (url.startsWith("jdbc:sqlserver")) {
            return new SqlServerMergeSqlGenerator();
        } else if (url.startsWith("jdbc:postgresql:")) {
            return new PostgresMergeSqlGenerator();
        } else {
            throw new IllegalStateException("database that can not use merge. database url: " + url);
        }
    }
}
