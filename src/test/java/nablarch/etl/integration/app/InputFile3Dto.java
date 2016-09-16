package nablarch.etl.integration.app;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import nablarch.common.databind.csv.Csv;
import nablarch.common.databind.csv.Csv.CsvType;
import nablarch.core.validation.ee.Domain;
import nablarch.etl.WorkItem;

/**
 * 入力ファイル3のBean。
 */
@Entity
@Table(name = "input_file3_table")
@Csv(type = CsvType.DEFAULT,
     headers = {"ユーザID", "名前", "列２", "列３"},
     properties = {"userId", "name", "col2", "col3"})
public class InputFile3Dto extends WorkItem {

    @Column(name = "user_id")
    @Domain("userId")
    public String userId;

    @Column(name = "name")
    public String name;

    @Column(name="col2")
    public String col2;

    @Column(name="col3")
    public String col3;

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
