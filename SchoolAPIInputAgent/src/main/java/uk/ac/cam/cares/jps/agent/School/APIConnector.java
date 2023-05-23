package main.java.uk.ac.cam.cares.jps.agent.School;


import org.apache.http.client.HttpResponseException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import uk.ac.cam.cares.jps.base.exception.JPSRuntimeException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


import java.io.*;
import java.util.Properties;

import javax.print.attribute.standard.JobHoldUntil;



public class APIConnector
{
    private String schoolGeneralURL = "https://data.gov.sg/api/action/datastore_search?resource_id=ede26d32-01af-4228-b1ed-f05c45a1d8ee&limit=346";
    private String schoolProgrammesURL="https://data.gov.sg/api/action/datastore_search?resource_id=9a94c7ed-710b-4ba5-8e01-8588f129efcc&limit=79";
    private String schoolCCAURL="https://data.gov.sg/api/action/datastore_search?resource_id=dd7a056a-49fa-4854-bd9a-c4e1a88f1181&limit=5422";
    

    private static final String ERRORMSG = "School data could not be retrieved";
    private static final Logger LOG = LogManager.getLogger(APIAgentLauncher.class);
  

    //Standard Constructor to initialise the instance variables
    
    public APIConnector(String GenURL, String ProgURL, String CCAURL)
    {
       schoolGeneralURL= GenURL;
       schoolProgrammesURL = ProgURL;
       schoolCCAURL = CCAURL;
    }
    

    //Constructor to initialise the variables according to the Properties file

    public APIConnector(String filepath) throws IOException
    {
        loadAPIConfigs(filepath);
    }      

    // Obtains School data in JSON format containing key:value pairs

    public JSONObject getGeneralReadings()
    {
        try{
            return retrieveGeneralData();
        }
        catch(IOException e)
        {
            LOG.error(ERRORMSG);
            throw new JPSRuntimeException(ERRORMSG,e);
        }
    }

    public JSONObject getProgrammes()
    {
        try
        {
            return retreiveProgrammes();
        }
        catch(Exception e)
        {
            LOG.error(ERRORMSG);
            throw new JPSRuntimeException(ERRORMSG,e);
        }
    }

    public JSONObject getCCA()
    {
        try
        {
            return retrieveCCA();
        }
        catch(Exception e)
        {
            LOG.error(ERRORMSG);
            throw new JPSRuntimeException(ERRORMSG,e);
        }
    }

    private JSONObject retrieveGeneralData() throws IOException, JSONException
    {  
        String path = schoolGeneralURL;

        try ( CloseableHttpClient httpclient =  HttpClients.createDefault())
        {
            HttpGet readrequest = new HttpGet(path);
            try ( CloseableHttpResponse response = httpclient.execute(readrequest))
            {
                int status = response.getStatusLine().getStatusCode();

                if(status==200) 
                {
                    return new JSONObject(EntityUtils.toString(response.getEntity()));

                }
                else
                {
                    throw new HttpResponseException(status,"Data could not be retrieved due to a server error");
                }

            }

        }

    }

    private JSONObject retreiveProgrammes() throws IOException, JSONException
    {
        String path = schoolProgrammesURL;

        try(CloseableHttpClient httpClient = HttpClients.createDefault())
        {
            HttpGet readrequest = new HttpGet(path);

            try(CloseableHttpResponse response = httpClient.execute(readrequest))
            {
                int status = response.getStatusLine().getStatusCode();

                if(status==200)
                {
                    return new JSONObject(EntityUtils.toString(response.getEntity()));
                }
                else
                {
                    throw new HttpResponseException(status,"Programmes Data could not be retrieved due to a server");
                }
            }
        }
    }

    private JSONObject retrieveCCA() throws IOException, JSONException
    {
        String path = schoolCCAURL;

        try(CloseableHttpClient httpClient = HttpClients.createDefault())
        {
            HttpGet readrequest = new HttpGet(path);

            try(CloseableHttpResponse response = httpClient.execute(readrequest))
            {
                int status = response.getStatusLine().getStatusCode();

                if(status==200)
                {
                    return new JSONObject(EntityUtils.toString(response.getEntity()));
                }
                else
                {
                    throw new HttpResponseException(status,"CCA Data could not be retrieved due to a server");
                }
            }
        }
    }


    private void loadAPIConfigs(String filepath) throws IOException
    {
        File file = new File(filepath);
        if(!file.exists())
        {
            throw new FileNotFoundException("There was no file found in the path");
        }
        
        try (InputStream input = new FileInputStream(file))
        {
            Properties prop = new Properties();
            prop.load(input);

            if(prop.containsKey("schoolgeneral.api_url"))
            {
                schoolGeneralURL = prop.getProperty("schoolgeneral.api_url");
            }
            else
            {
                throw new IOException("The file is missing: \"schoolgeneral.api_url=<api_url>\"");
            }

            if(prop.containsKey("schoolProgrammes.api_url"))
            {
                schoolProgrammesURL = prop.getProperty("schoolProgrammes.api_url");
            }
            else
            {
                throw new IOException("The file is missing: \"schoolProgrammes.api_url=<api_url>\"");
            }

            if(prop.containsKey("schoolCCA.api_url"))
            {
                schoolCCAURL = prop.getProperty("schoolCCA.api_url");
            }
            else
            {
                throw new IOException("The file is missing: \"schoolCCA.api_url=<api_url>\"");
            }
            

        }
    }

}
