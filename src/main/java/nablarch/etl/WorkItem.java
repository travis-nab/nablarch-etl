package nablarch.etl;

import javax.persistence.Column;
import javax.persistence.Id;

import nablarch.common.databind.LineNumber;
import nablarch.core.util.annotation.Published;

/**
 * 全てのワークテーブルに共通するプロパティを保持するクラス。
 * <p/>
 * ワークテーブルのEntityは本クラスを継承する。
 *
 * @author Kumiko Omi
 */
@Published
public abstract class WorkItem {

    /** 行数を保持するカラム */
    private Long lineNumber;

    /**
     * 行数を取得する。
     *
     * @return 行数
     */
    @Id
    @LineNumber
    @Column(name = "LINE_NUMBER", length = 18, nullable = false, unique = true)
    public Long getLineNumber() {
        return lineNumber;
    }

    /**
     * 行数を設定する。
     * @param lineNumber 行数
     */
    public void setLineNumber(Long lineNumber) {
        this.lineNumber = lineNumber;
    }
}
