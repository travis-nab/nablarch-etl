package nablarch.etl;

import nablarch.core.util.annotation.Published;

/**
 * バリデーションエラーが発生し、ステップを異常終了することを示す例外クラス。
 *
 * @author Hisaaki Shiori
 */
@Published(tag = "architect")
public class EtlJobAbortedException extends RuntimeException {

    /**
     * 例外メッセージを持つ{@code EtlJobAborted}を生成する。
     *
     * @param message 例外メッセージ
     */
    public EtlJobAbortedException(final String message) {
        super(message);
    }
}
