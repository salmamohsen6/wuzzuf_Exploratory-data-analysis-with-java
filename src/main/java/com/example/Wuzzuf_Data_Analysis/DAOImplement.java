package com.example.Wuzzuf_Data_Analysis;
import org.apache.spark.sql.*;
import org.springframework.stereotype.Component;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.knowm.xchart.*;
import org.apache.spark.api.java.JavaRDD;
import org.knowm.xchart.style.Styler;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;
//https://stackoverflow.com/questions/64560920/nosuchfieldexception-while-creating-a-spark-session-using-builder

@Component
public class DAOImplement implements DAO{
    static List<POJO> jobs=null;
    String path;
    SparkSession sparkSession;

    Dataset<Row> dataset;
    public DAOImplement() {
        this.path = "src/main/resources/Wuzzuf_Jobs.csv";
    }
    public DAOImplement(String path) {
        this.path = path;
    }
    //public void setPath(String path) {this.path = path;}
    public Dataset<Row> getDataset() {
        return dataset;
    }
    public POJO readCSV() {
        sparkSession = SparkSession.builder()
                .appName("Spark CSV Analysis Demo")
                .master("local[3]")
                .getOrCreate();
        sparkSession.sparkContext().setLogLevel("ERROR");
        dataset= sparkSession.read().option("header",true).csv(path);
        this.cleanDataSet();
        return new POJO(dataset);
    }

    public void cleanDataSet(){
        Dataset<Row> wuzzufDFNoNullDF=dataset.na().drop ();
        this.dataset= wuzzufDFNoNullDF.dropDuplicates();
    }
    public POJO countJobs(){
        return new POJO(dataset.groupBy("Company").count().sort(col("count").desc()));
    }
    Color getRandomColors(){
        Random rand = new Random();
        int upperbound = 255;
        int r = rand.nextInt(upperbound);
        int g= rand.nextInt(upperbound);
        int b = rand.nextInt(upperbound);
        return new Color(r,g,b);
    }
    public void displayPieChart(Dataset<Row> data){
        PieChart chart = new PieChartBuilder().width (800).height (600).title ("jobs for each company").build ();
        List<List<Row>> columnLists = new ArrayList<>();
        String[] columnNames = data.columns();
        List<String> dataObjects1 = new ArrayList<>();
        List<Long> dataObjects2 = new ArrayList<>();

        for (String col : columnNames) {
            columnLists.add(data.select(col).collectAsList().stream().collect(Collectors.toList()));
        }
        for (int i=0 ; i < columnLists.get(0).size() ; i++) {
            String jobs1 = columnLists.get(0).get(i).get(0).toString();
            dataObjects1.add(jobs1);
            Long count = (Long) columnLists.get(1).get(i).get(0);
            dataObjects2.add(count);
        }
        Color[] colorArray = new Color[dataObjects1.size()];
        int size=dataObjects2.size ();
        //System.out.println(columnLists.size()+" "+dataObjects1.size() + " " + dataObjects2.size());


        for (int i=0;i<10;i++) {
            colorArray[i] =getRandomColors();
            chart.addSeries(dataObjects1.get(i), dataObjects2.get(i));
        }
        chart.getStyler ().setSeriesColors (colorArray);
        //new SwingWrapper(chart).displayChart ();
        try {
            BitmapEncoder.saveJPGWithQuality(chart, "src/main/resources/PieChart.png", 0.95f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void displayBarChart(Dataset<Row> data, boolean flag){
        String title,xaxis,yaxis,name, fileName;
        if(!flag){
            title = "the most popular job titles";
            xaxis = "job titles";
            yaxis= "job counts";
            name = "no. of jobs for each title";
            fileName = "mostPopTitles.png";
        }
        else{
            title = "the most popular areas";
            xaxis = "area";
            yaxis= "job counts";
            name = "no. of jobs for each area";
            fileName = "mostPopAreas.png";
        }
        //bar chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).xAxisTitle(xaxis).yAxisTitle(yaxis).build();
        // List contain all columns as lists
        List<List<Row>> columnLists = new ArrayList<>();
        // Array of Column names
        String[] columnNames = data.columns();
        // List of Data objects
        List<String> dataObjects1 = new ArrayList<>();
        List<Long> dataObjects2 = new ArrayList<>();


        // construct list of Columns Lists
        for (String col : columnNames) {
            columnLists.add(data.select(col).collectAsList().stream().collect(Collectors.toList()));
        }

        // construct Data Objects List
        for (int i=0 ; i < 10 ; i++) {
            String jobs3 = columnLists.get(0).get(i).get(0).toString();
            dataObjects1.add(jobs3);
            Long count2 = (Long) columnLists.get(1).get(i).get(0);
            dataObjects2.add(count2);
        }
        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);

        // Series
        chart.addSeries(name, dataObjects1,dataObjects2);
        //for (int i=0;i<10;i++) {
        //     chart1.addSeries("test 1",dataObjects3.get(i), dataObjects4.get(i));
        //}
        // Show it
        //new SwingWrapper(chart).displayChart ();
        try {
            BitmapEncoder.saveJPGWithQuality(chart, "src/main/resources/"+fileName, 0.95f);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public POJO mostPopularJobs(){
        return new POJO(dataset.groupBy("Title").count().sort(col("count").desc()));
    }
    public POJO mostPopularAreas(){
        return new POJO( dataset.groupBy("Location").count().sort(col("count").desc()));
    }
    public POJO mostPopularSkills(){
        final Dataset<Row> skills = dataset.select(split(col("Skills"),",").as("Skills"));

        List<List<String>> tempList = new ArrayList<>();
        for(Row row :skills.collectAsList())
        {
            tempList.add(row.<String>getList(0));
        }
        List<String> skillsList = tempList.stream().flatMap(List::stream).map(String::trim).collect(Collectors.toList());
        //System.out.println(skillsList);
        Dataset<String> skillsTemp = sparkSession.createDataset(skillsList , Encoders.STRING());
        Dataset<Row> skillsDF = skillsTemp.select(col("value").as("Skills"));
        //skillsDF.show();
        //System.out.println(skillsDF.count());

        //mostImportantSkills.select("Skills").foreach((ForeachFunction<Row>) row -> System.out.println(row.get(0) + " " + row.get(0).toString().length()));
        //mostImportantSkills.show();
        return new POJO(skillsDF.groupBy("Skills").count().sort(col("count").desc()));
    }
    public POJO getJobByTitle(String Title)
    {
        for ( POJO job: jobs)
        {
            if (job.getTitle()==Title)
            {
                return job;
            }
        }
        return null;
    }

    @Override
    public boolean add(POJO j) {
        try {
            jobs.add(j);
        }
        catch(Exception e)
        {
            return false;
        }
        return true;
    }

    @Override
    public boolean update(POJO j) {
        for (POJO job: jobs)
        {
            if (job.getTitle()==j.getTitle())
            {
                job.setTitle(j.getTitle());
                job.setCompany(j.getCompany());
                job.setLocation(j.getLocation());
                job.setLevel(j.getLevel());
                job.setType(j.getType());
                job.setYearsExp(j.getYearsExp());
                job.setCountry(j.getCountry());
                job.setSkills(j.getSkills());
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean delete(String Title) {
        POJO j=getJobByTitle(Title);
        return jobs.remove(j);
    }

    @Override
    public POJO get(String Title) {
        for (POJO job: jobs)
        {
            if (job.getTitle()==Title)
            {
                return job;
            }
        }
        return null;    }

    @Override
    public List<POJO> getAll() {
        return jobs;
    }
}
