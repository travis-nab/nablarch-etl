package nablarch.etl.integration;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "input_file1_errors")
public class InputFile1ErrorEntity {

    @Id
    @Column(name = "line_number", length = 15)
    public Long lineNumber;

    @Column(name = "user_id")
    public String userId;

    @Column(name = "name")
    public String name;

    public InputFile1ErrorEntity() {
    }

    public InputFile1ErrorEntity(Long lineNumber, String userId, String name) {
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
