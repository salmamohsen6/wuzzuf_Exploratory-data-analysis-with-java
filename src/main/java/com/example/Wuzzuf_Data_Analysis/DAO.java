package com.example.Wuzzuf_Data_Analysis;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
public interface DAO {
    boolean add(POJO j);
    boolean update(POJO j);
    boolean delete(String Title);
    POJO get(String Title);
    List<POJO> getAll();
    public void cleanDataSet();
}