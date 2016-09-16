package nablarch.etl.integration.app;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * 出力先テーブル1のエンティティ。
 */
@Entity
@Table(name = "output_table1")
public class OutputTable1Entity {

    @Id
    @Column(name = "user_id")
    public String userId;

    @Column(name = "name")
    public String name;

    public OutputTable1Entity() {
    }

    public OutputTable1Entity(String userId, String name) {
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
