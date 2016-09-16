package nablarch.etl.integration;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 入力ファイル2を格納するワークテーブル。
 */
@Entity
@Table(name = "input_file2_table")
public class InputFile2WorkTable {

    @Id
    @Column(name = "line_number", length = 15)
    public Long lineNumber;

    @Column(name="col1", length=5)
    public String col1;

    @Column(name="col2", length=10)
    public String col2;

    @Column(name="col3", length=5)
    public String col3;

    public InputFile2WorkTable() {
    }

    public InputFile2WorkTable(Long lineNumber, String col1, String col2, String col3) {
        this.lineNumber = lineNumber;
        this.col1 = col1;
        this.col2 = col2;
        this.col3 = col3;
    }

    public String getCol1() {
        return col1;
    }

    public void setCol1(String col1) {
        this.col1 = col1;
    }

    public String getCol2() {
        return col2;
    }

    public void setCol2(String col2) {
        this.col2 = col2;
    }

    public String getCol3() {
        return col3;
    }

    public void setCol3(String col3) {
        this.col3 = col3;
    }
}

