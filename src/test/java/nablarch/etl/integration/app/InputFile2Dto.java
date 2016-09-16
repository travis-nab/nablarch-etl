package nablarch.etl.integration.app;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import nablarch.common.databind.csv.Csv;
import nablarch.common.databind.csv.Csv.CsvType;
import nablarch.core.validation.ee.Domain;
import nablarch.etl.WorkItem;

/**
 * 入力ファイル2のBean。
 */
@Entity
@Table(name = "input_file2_table")
@Csv(type = CsvType.DEFAULT, headers = {"列１", "列２", "列３"}, properties = {"col1", "col2", "col3"})
public class InputFile2Dto extends WorkItem {

    @Column(name="col1")
    public String col1;

    @Column(name="col2")
    public String col2;

    @Column(name="col3")
    public String col3;

    @Domain("userId")
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
