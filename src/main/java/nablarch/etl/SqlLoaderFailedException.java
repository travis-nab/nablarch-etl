package nablarch.etl;

/**
 * SQL*Loaderの実行に失敗した場合に送出される例外クラス。
 *
 * @author Naoki Yamamoto
 */
public class SqlLoaderFailedException extends RuntimeException {

    /** serialVersionUID */
    private static final long serialVersionUID = 1L;

    /**
     * 例外を生成する。
     *
     * @param msg 例外のメッセージ
     */
    public SqlLoaderFailedException(String msg) {
        super(msg);
    }

}
