package nablarch.etl;

import nablarch.core.util.annotation.Published;

/**
 * 一定間隔の範囲を提供するクラス。
 *
 * @author Kiyohito Itoh
 */
@Published(tag = "architect")
public class Range {

    /** 間隔 */
    final int interval;

    /** 最大位置 */
    final long max;

    /** 開始位置 */
    long from;

    /** 終了位置 */
    long to;

    /**
     * コンストラクタ。
     * @param interval 間隔
     * @param max 最大位置
     */
    Range(final int interval, final long max) {
        this.interval = interval;
        this.max = max;
        from = 1;
        to = 0;
    }

    /**
     * 次の間隔に進める。
     * @return 次の間隔に進めた場合は<code>true</code>。既に最大位置に達している場合は<code>false</code>
     */
    boolean next() {
        if (to == max) {
            return false;
        }
        from = to + 1;
        to += interval;
        if (to > max) {
            to = max;
        }
        return true;
    }
}
