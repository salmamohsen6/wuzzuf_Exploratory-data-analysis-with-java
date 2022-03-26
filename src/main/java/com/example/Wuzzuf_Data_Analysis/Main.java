package com.example.Wuzzuf_Data_Analysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.IOException;

@SpringBootApplication
public class Main {

    public static void main(String[] args) throws IOException {
        //SpringApplication.run(Main.class, args);
        SpringApplicationBuilder builder = new SpringApplicationBuilder(Main.class);
        builder.headless(false);
        ConfigurableApplicationContext context = builder.run(args);

        ////////////////1- read dataset and convert it to spark dataset and display some from it/////////
        String path = "src/main/resources/Wuzzuf_Jobs.csv";
        DAOImplement wuzzufDao = new DAOImplement(path);

        POJO readDataSet = wuzzufDao.readCSV();
        System.out.println("===============display some from dataset=================");
        Dataset<Row> wuzzufDataSet = readDataSet.getDataset();
        wuzzufDataSet.show(10);

        /////////////2- display structure and summary of data
        System.out.println("===============DataSet Summary==============");
        System.out.println("Total count: " + wuzzufDataSet.count());
        wuzzufDataSet.describe().show();
        System.out.println("===============DataSet Schema==============");
        wuzzufDataSet.printSchema();

        ///////////3- clean data(null , duplications)
        wuzzufDao.cleanDataSet();
        wuzzufDataSet = wuzzufDao.getDataset();
        System.out.println("Total count after cleaning data: " + wuzzufDataSet.count());

        ////////////4-count the jobs for each company and display that in order
        System.out.println("===============count the jobs for each company and display that in order==============");
        POJO jobsForCompany = wuzzufDao.countJobs();
        Dataset<Row> jobsForCompanyDataSet = jobsForCompany.getDataset();
        jobsForCompanyDataSet.show();

        ///////////5- show step 4 in pie chart///////////////
        wuzzufDao.displayPieChart(jobsForCompanyDataSet);

        ///////////6- find out what are the most popular job titles//////////
        System.out.println("===============the most popular job titles==============");
        POJO popularJobTitles = wuzzufDao.mostPopularJobs();
        Dataset<Row> popularJobTitlesDataSet = popularJobTitles.getDataset();
        popularJobTitlesDataSet.show();

        /////////7- show step 6 in bar chart/////////////
        wuzzufDao.displayBarChart(popularJobTitlesDataSet,false);

        /////////8- find out the most popular areas ///////////
        System.out.println("===============the most popular areas==============");
        POJO popularAreas = wuzzufDao.mostPopularAreas();
        Dataset<Row> popularAreasDataSet = popularAreas.getDataset();
        popularAreasDataSet.show();

        /////////9- show step 8 in bar chart/////////////
        wuzzufDao.displayBarChart(popularAreasDataSet,true);

        //////// 10- find out the most important skills //////////
        System.out.println("===============he most important skills==============");
        POJO importantSkills = wuzzufDao.mostPopularSkills();
        Dataset<Row> importantSkillsDataSet = importantSkills.getDataset();
        importantSkillsDataSet.show();



    }
}
