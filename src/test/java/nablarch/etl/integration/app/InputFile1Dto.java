package nablarch.etl.integration.app;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import nablarch.common.databind.csv.Csv;
import nablarch.common.databind.csv.Csv.CsvType;
import nablarch.core.validation.ee.Domain;
import nablarch.etl.WorkItem;

/**
 * 入力ファイル1のBean。
 */
@Entity
@Table(name = "input_file1_table")
@Csv(type = CsvType.DEFAULT, headers = {"ユーザID", "名前"}, properties = {"userId", "name"})
public class InputFile1Dto extends WorkItem {

    public String userId;

    public String name;

    @Domain("userId")
    @Column(name = "user_id")
    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Column(name = "name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
