package nablarch.etl.integration;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 入力ファイル1を格納するワークテーブル。
 */
@Entity
@Table(name = "input_file1_table")
public class InputFile1WorkTable {

    @Id
    @Column(name = "line_number", length = 15)
    public Long lineNumber;

    @Column(name = "user_id")
    public String userId;

    @Column(name = "name")
    public String name;

    public InputFile1WorkTable() {
    }

    public InputFile1WorkTable(Long lineNumber, String userId, String name) {
        this.lineNumber = lineNumber;
        this.userId = userId;
        this.name = name;
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
}

