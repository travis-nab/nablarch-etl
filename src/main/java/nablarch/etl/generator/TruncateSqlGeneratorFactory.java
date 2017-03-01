package nablarch.etl.generator;

import nablarch.core.db.connection.TransactionManagerConnection;
import nablarch.core.util.StringUtil;
import nablarch.etl.EtlUtil;

/**
 * TRUNCATE文を生成するジェネレータのファクトリクラス。
 * <p/>
 * データベース接続のURLを元に、TRUNCATE文のジェネレータクラスを生成する。
 *
 * @author Naoki Yamamoto
 */
public final class TruncateSqlGeneratorFactory {

    /**
     * 隠蔽コンストラクタ。
     */
    private TruncateSqlGeneratorFactory() {
    }

    /**
     * TRUNCATE文を生成するジェネレータを生成する。
     *
     * @param connection データベース接続
     * @return TRUNCATE文を生成するジェネレータ
     */
    public static TruncateSqlGenerator create(final TransactionManagerConnection connection) {
        final String url = EtlUtil.getUrl(connection);
        if (StringUtil.isNullOrEmpty(url)) {
            throw new IllegalStateException("failed to get connection url.");
        }
        return url.startsWith("jdbc:db2") ? new Db2TruncateSqlGenerator() : new TruncateSqlGenerator();
    }
}
