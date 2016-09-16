package nablarch.etl.integration;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 入力ファイル3を格納するワークテーブル。
 */
@Entity
@Table(name = "input_file3_table")
public class InputFile3WorkTable {

    @Id
    @Column(name = "line_number", length = 15)
    public Long lineNumber;

    @Column(name = "user_id")
    public String userId;

    @Column(name = "name")
    public String name;

    @Column(name="col2", length=10)
    public String col2;

    @Column(name="col3", length=5)
    public String col3;

    public InputFile3WorkTable() {
    }

    public InputFile3WorkTable(String userId, String name, String col2, String col3) {
        this.userId = userId;
        this.name = name;
        this.col2 = col2;
        this.col3 = col3;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
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
