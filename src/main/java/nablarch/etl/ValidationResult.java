package nablarch.etl;

/**
 * validation結果を保持するクラス。
 *
 * @author Hisaaki Shioiri
 */
class ValidationResult {

    /** 行数 */
    private long lineCount;

    /** エラー数 */
    private long errorCount;

    /**
     * 行数をインクリメントする。
     */
    void incrementCount() {
        lineCount++;
    }

    /**
     * エラー数をインクリメントする。
     *
     * @param count 加算するエラー数
     */
    void addErrorCount(final int count) {
        errorCount += count;
    }


    /**
     * エラー数を取得する。
     *
     * @return エラー数
     */
    long getErrorCount() {
        return errorCount;
    }

    /**
     * 行数を取得する。
     * @return 行数
     */
    public long getLineCount() {
        return lineCount;
    }

    /**
     * エラーの有無を取得する。
     *
     * @return エラーがある場合はtrue
     */
    boolean hasError() {
        return errorCount != 0;
    }
}
