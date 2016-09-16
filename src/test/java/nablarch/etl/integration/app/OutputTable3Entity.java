package nablarch.etl.integration.app;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 出力先テーブル3のエンティティ。
 */
@Entity
@Table(name = "output_table3")
public class OutputTable3Entity {

    @Id
    @Column(name = "user_id")
    public String userId;

    @Column(name = "name")
    public String name;

    @Column(name="col2")
    public String col2;

    @Column(name="col3", length = 5)
    public Integer col3;

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

    public Integer getCol3() {
        return col3;
    }

    public void setCol3(Integer col3) {
        this.col3 = col3;
    }
}
