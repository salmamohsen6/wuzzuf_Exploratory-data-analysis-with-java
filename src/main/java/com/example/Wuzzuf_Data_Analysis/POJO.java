package com.example.Wuzzuf_Data_Analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class POJO {

        String Title;
        String Company;
        String Location;
        String Type;
        String Level;
        String YearsExp;
        String Country;
        String Skills;
    private Dataset<Row> dataset;
    public POJO(Dataset<Row> dataset){
        this.dataset = dataset;
    }
    public void cleanDataSet(){
        Dataset<Row> wuzzufDFNoNullDF=dataset.na().drop ();
        this.dataset= wuzzufDFNoNullDF.dropDuplicates();
    }
    public POJO(String Title,String Company,String Location,String Type,String Level,String YearsExp,String Country,String Skills)
        {
            this.Title = Title;
            this.Company = Company;
            this.Location = Location;
            this.Type = Type;
            this.Level = Level;
            this.YearsExp = YearsExp;
            this.Country = Country;
            this.Skills = Skills;
        }
        public Dataset<Row> getDataset() {
            return dataset;
        }
        public void setDataset(Dataset<Row> dataset) {
            this.dataset = dataset;
        }

        public String getTitle () {
            return Title;
        }

        public void setTitle (String Title){
            this.Title = Title;
        }

        public String getCompany () {
            return Company;
        }

        public void setCompany (String Company){
            this.Company = Company;
        }

        public String getLocation () {
            return Location;
        }

        public void setLocation (String Location){
            this.Location = Location;
        }

        public String getType () {
            return Type;
        }

        public void setType (String Type){
            this.Type = Type;
        }

        public String getLevel () {
            return Level;
        }

        public void setLevel (String Level){
            this.Level = Level;
        }

        public String getYearsExp () {
            return YearsExp;
        }

        public void setYearsExp (String YearsExp){
            this.YearsExp = YearsExp;
        }

        public String getCountry () {
            return Country;
        }

        public void setCountry (String Country){
            this.Country = Country;
        }

        public String getSkills () {
            return Skills;
        }

        public void setSkills (String Skills){
            this.Skills = Skills;
        }


        @Override
        public String toString () {
            return " [Title=" + Title + ", Company=" + Company + ", Location=" + Location + ", Type="
                    + Type + ", Level=" + Level + ", YearsExp=" + YearsExp + ", Country=" + Country + ", Skills=" + Skills + "]";
        }

    public void removeNulls() {
        Dataset<Row> wuzzufDFNoNullDF=dataset.na().drop ();
        this.dataset= wuzzufDFNoNullDF.dropDuplicates();
    }
}

