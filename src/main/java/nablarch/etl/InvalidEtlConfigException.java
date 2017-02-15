package nablarch.etl;

/**
 * ETLの設定が無効であることを示す例外クラス。
 *
 * @author siosio
 */
public class InvalidEtlConfigException extends RuntimeException {

    /**
     * メッセージを元に例外を構築する。
     *
     * @param message メッセージ
     */
    public InvalidEtlConfigException(String message) {
        super(message);
    }
}
