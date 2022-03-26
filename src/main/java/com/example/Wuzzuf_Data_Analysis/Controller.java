package com.example.Wuzzuf_Data_Analysis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.springframework.http.MediaType.IMAGE_JPEG_VALUE;

@RestController
public class Controller {
    @RequestMapping("/hello")
    public String ay7aga(){
        return "hello";
    }
    @Autowired
    private Services services;

    @RequestMapping(value = "/PreliminaryStatistic", produces = "application/json")
    public List<Map> statistics ()  {
        return services.getStats();
    }

    @RequestMapping("/HeadOfTheDataSet")
    public List<Map> header ()  {
        return  services.getHeader();
    }

    @RequestMapping("/RemovingNullValuesAndDuplicates")
    public List<Map> RemovaNulls ()  {
        return services.cleanData();
    }

    @RequestMapping("/DataSetSchema")
    public List<Map> getSchema ()  {
        return services.getSchema();
    }

    @RequestMapping("/CompanyOffers")
    public List<Map> getCompanyOffers ()  {
        return services.getCompanyOffers();
    }

    @RequestMapping("/MostPopularJobs")
    public List<Map> getMostPopJobs ()  {
        return services.getMostPopJobs();
    }

    @RequestMapping("/MostPopularAreas")
    public List<Map> getMostPopAreas ()  {
        return services.getMostPopAreas();
    }

    @RequestMapping("/MostImportantSkills")
    public List<Map> getMostRepeatedSkills()  {
        return services.mostRepSkills();
    }


    @RequestMapping (value = "CompanyOffersPieChart", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<Resource> companyOffers() throws IOException {
        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "src/main/resources/PieChart.png")));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }
    @RequestMapping (value = "popular-title-chart-image", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<Resource> popularTitlesChartImage() throws IOException {
        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "src/main/resources/mostPopTitles.png")));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }
    @RequestMapping (value = "popular-areas-chart-image", produces = MediaType.IMAGE_PNG_VALUE)
    public ResponseEntity<Resource> popularareasChartImage() throws IOException {
        final ByteArrayResource inputStream = new ByteArrayResource(Files.readAllBytes(Paths.get(
                "src/main/resources/mostPopAreas.png")));
        return ResponseEntity
                .status(HttpStatus.OK)
                .contentLength(inputStream.contentLength())
                .body(inputStream);
    }

    /*@RequestMapping(value = "/CompanyOffersPieChart", method = RequestMethod.GET, produces = IMAGE_JPEG_VALUE)
    public void getCompanyOffersPieChart(HttpServletResponse response) throws IOException {

        services.PieChartPath();
        ClassPathResource imgFile = new ClassPathResource("companyOffers.jpg");

        response.setContentType(IMAGE_JPEG_VALUE);
        StreamUtils.copy(imgFile.getInputStream(), response.getOutputStream());
    }

    @RequestMapping(value = "/mostPopularJobsCategoryChart", method = RequestMethod.GET, produces = IMAGE_JPEG_VALUE)
    public void getMostPopJobsCatChart(HttpServletResponse response) throws IOException {

        services.jobsCategoryChart();
        ClassPathResource imgFile = new ClassPathResource("jobs.jpg");

        response.setContentType(IMAGE_JPEG_VALUE);
        StreamUtils.copy(imgFile.getInputStream(), response.getOutputStream());
    }

    @RequestMapping(value = "/mostPopularAreaCategoryChart", method = RequestMethod.GET, produces = IMAGE_JPEG_VALUE)
    public void getMostPopAreasCatChart(HttpServletResponse response) throws IOException {

        services.areaCategoryChart();
        ClassPathResource imgFile = new ClassPathResource("Areas.jpg");

        response.setContentType(IMAGE_JPEG_VALUE);
        StreamUtils.copy(imgFile.getInputStream(), response.getOutputStream());
    }*/


}

