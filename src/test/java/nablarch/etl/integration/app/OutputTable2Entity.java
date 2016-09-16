package nablarch.etl.integration.app;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 出力先テーブル2のエンティティ。
 */
@Entity
@Table(name = "output_table2")
public class OutputTable2Entity {

    @Id
    @Column(name="col1")
    public String col1;

    @Column(name="col2")
    public String col2;

    @Column(name="col3", length = 5)
    public Integer col3;

    public OutputTable2Entity() {
    }

    public OutputTable2Entity(String col1, String col2, Integer col3) {
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

    public Integer getCol3() {
        return col3;
    }

    public void setCol3(Integer col3) {
        this.col3 = col3;
    }
}
