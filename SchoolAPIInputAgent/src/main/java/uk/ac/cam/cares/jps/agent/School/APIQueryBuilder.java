package main.java.uk.ac.cam.cares.jps.agent.School;


import org.json.JSONArray;
import org.json.JSONObject;
import org.jooq.exception.DataAccessException;
import uk.ac.cam.cares.jps.base.util.JSONKeyToIRIMapper;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeries;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesClient;
import uk.ac.cam.cares.jps.base.timeseries.TimeSeriesSparql;
import me.xdrop.fuzzywuzzy.FuzzySearch;
import java.text.SimpleDateFormat;
import uk.ac.cam.cares.jps.base.exception.JPSRuntimeException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import javax.lang.model.util.ElementScanner6;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.rdf4j.sparqlbuilder.core.Prefix;
import org.eclipse.rdf4j.sparqlbuilder.core.SparqlBuilder;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.TriplePattern;
import org.eclipse.rdf4j.sparqlbuilder.rdf.Iri;
import org.eclipse.rdf4j.sparqlbuilder.core.Variable;
import org.eclipse.rdf4j.sparqlbuilder.core.query.SelectQuery;

import org.json.JSONArray;

import uk.ac.cam.cares.jps.base.query.RemoteStoreClient;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import static org.eclipse.rdf4j.sparqlbuilder.rdf.Rdf.iri;

import org.eclipse.rdf4j.sparqlbuilder.core.query.DeleteDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.InsertDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
public class APIQueryBuilder
{
    

    public String queryEndpoint;
    public String updateEndpoint;

    RemoteStoreClient kbClient;

    /**
     * Namespaces for ontologies
     */

    public static final String OntoSchool = "https://www.theworldavatar.com/kg/ontoschool/";
    public static final String RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#";

    
    
	/**
     * Prefixes
     */ 

    private static final Prefix PREFIX_ONTOSCHOOL = SparqlBuilder.prefix("ontoSchool", iri(OntoSchool));
    public static final String generatedIRIPrefix = TimeSeriesSparql.TIMESERIES_NAMESPACE + "School";
    private static final Prefix PREFIX_RDFS = SparqlBuilder.prefix("rdfs", iri(RDFS_NS));

    
    
	/**
     * Relationships
    */ 

    private static final Iri hasAddress = PREFIX_ONTOSCHOOL.iri("hasAddress");
    private static final Iri hasName = PREFIX_ONTOSCHOOL.iri("hasName");
    private static final Iri hasEmailAddress = PREFIX_ONTOSCHOOL.iri("hasEmailAddress");
    private static final Iri hasPostalCode = PREFIX_ONTOSCHOOL.iri("hasPostalCode");
    private static final Iri hasTelephoneNumber = PREFIX_ONTOSCHOOL.iri("hasTelephoneNumber");
    private static final Iri hasProgramme = PREFIX_ONTOSCHOOL.iri("hasProgramme");
    private static final Iri hasSchoolType = PREFIX_ONTOSCHOOL.iri("hasSchoolType");
    private static final Iri hasStudentClassification = PREFIX_ONTOSCHOOL.iri("hasStudentClassification");
    private static final Iri hasCCAGroup = PREFIX_ONTOSCHOOL.iri("hasCCAGroup");
    private static final Iri hasCCA = PREFIX_ONTOSCHOOL.iri("hasCCA");

    

    /**
     * Classes
    */

    private static final Iri Name = PREFIX_ONTOSCHOOL.iri("Name");
    private static final Iri School = PREFIX_ONTOSCHOOL.iri("School");
    private static final Iri Address = PREFIX_ONTOSCHOOL.iri("Address");
    private static final Iri EmailAddress = PREFIX_ONTOSCHOOL.iri("EmailAddress");
    private static final Iri PostalCode = PREFIX_ONTOSCHOOL.iri("PostalCode");
    private static final Iri TelephoneNumber = PREFIX_ONTOSCHOOL.iri("TelephoneNumber");
    private static final Iri Programme = PREFIX_ONTOSCHOOL.iri("Programme");
    private static final Iri SchoolType = PREFIX_ONTOSCHOOL.iri("SchoolType");
    private static final Iri StudentClassification = PREFIX_ONTOSCHOOL.iri("StudentClassification");
    private static final Iri COED = PREFIX_ONTOSCHOOL.iri("CO-ED");
    private static final Iri Boys = PREFIX_ONTOSCHOOL.iri("Boys");
    private static final Iri Girls = PREFIX_ONTOSCHOOL.iri("Girls");
    private static final Iri Government = PREFIX_ONTOSCHOOL.iri("Government");
    private static final Iri GovernmentAided = PREFIX_ONTOSCHOOL.iri("Government-Aided");
    private static final Iri Specialised = PREFIX_ONTOSCHOOL.iri("Specialised");
    private static final Iri Independent = PREFIX_ONTOSCHOOL.iri("Independent");
    private static final Iri SpecialisedIndependent = PREFIX_ONTOSCHOOL.iri("SpecialisedIndependent");
    private static final Iri CCAGroup = PREFIX_ONTOSCHOOL.iri("CCAGroup");
    private static final Iri ClubsSocieties = PREFIX_ONTOSCHOOL.iri("Clubs&Societies");
    private static final Iri Others = PREFIX_ONTOSCHOOL.iri("Others");
    private static final Iri UniformedGroups = PREFIX_ONTOSCHOOL.iri("UniformedGroups");
    private static final Iri PhysicalSports= PREFIX_ONTOSCHOOL.iri("PhysicalSports");
    private static final Iri VisualandPerformingArta = PREFIX_ONTOSCHOOL.iri("VisualandPerformingArts");
    
    
     


    public String agentProperties;
    public String clientProperties;
    public JSONObject schoolReadings;
    public JSONObject programmeReadings;
    public JSONObject ccaReadings;

    private List<JSONKeyToIRIMapper> mappings;

    public APIQueryBuilder(String agentProp, String clientProp) throws IOException
    {
        agentProperties = agentProp;
        clientProperties = clientProp;


        loadconfigs(clientProperties);
        //readings endpoints from client.properties

        loadproperties(agentProperties);
        
        kbClient = new RemoteStoreClient();

        kbClient.setUpdateEndpoint(updateEndpoint);
        kbClient.setQueryEndpoint(queryEndpoint);


    }

    public void loadproperties(String propfile) throws IOException
    {
        try(InputStream input = new FileInputStream(propfile))
        {
            Properties prop = new Properties();
            prop.load(input);

            String mappingfolder;

            try
            {
                mappingfolder = System.getenv(prop.getProperty("School.mappingfolder"));
            }
            catch(NullPointerException e)
            {
                throw new IOException("The key School.mappingfolder cannot be found");
            }

            if(mappingfolder == null)
            {
                throw new InvalidPropertiesFormatException("The properties file does not contain the key School.mappingfolder with a path to the folder containing the required JSON key to IRI Mappings");
            }

            mappings = new ArrayList<>();
            File folder = new File(mappingfolder);
            File[] mappingFiles = folder.listFiles();

            if(mappingFiles.length == 0)
            {
                throw new IOException("No files in folder");
            }

            else
            {
                for(File mappingFile : mappingFiles)
                {
                    JSONKeyToIRIMapper mapper = new JSONKeyToIRIMapper(generatedIRIPrefix, mappingFile.getAbsolutePath());
                    mappings.add(mapper);
                    mapper.saveToFile(mappingFile.getAbsolutePath());
                }
            }

        }


    }

    public void loadconfigs(String filepath) throws IOException
    {
        File file = new File(filepath);
        if(!file.exists())
        {
            throw new FileNotFoundException("There was no file found in the path");
        }
        
        try(InputStream input = new FileInputStream(file))
        {
            Properties prop = new Properties();
            prop.load(input);

            if(prop.containsKey("sparql.query.endpoint"))
            {
                queryEndpoint = prop.getProperty("sparql.query.endpoint");
            }
            else
            {
                throw new IOException("The file is missing: \"sparql.query.endpoint=<queryEndpoint>\"");
            }

            if(prop.containsKey("sparql.update.endpoint"))
            {
                updateEndpoint = prop.getProperty("sparql.update.endpoint");
            }
            else
            {
                throw new IOException("The file is missing: \"sparql.update.endpoint=<updateEndpoint>\"");
            }
        }
    }


    public void instantiateIfNotInstantiated(JSONObject schools, JSONObject programmes, JSONObject CCA)
    {
        
        schoolReadings=schools;
        programmeReadings = programmes;
        ccaReadings=CCA;
        

        List<String> iris;

        JSONArray programmeArray = programmeReadings.getJSONObject("result").getJSONArray("records");
        JSONArray ccaArray =  ccaReadings.getJSONObject("result").getJSONArray("records");

        for(JSONKeyToIRIMapper mapping : mappings)
        {
            iris = mapping.getAllIRIs();
           
           for(String iri:iris)
           if(!iri.contains("School_time_"))
           {
                String r=null;
                Variable s = SparqlBuilder.var("s");
                SelectQuery q = Queries.SELECT();
    
                //Create a triplePattern
    
                TriplePattern qP = iri(iri).isA(s);
                q.prefix(PREFIX_ONTOSCHOOL).select(s).where(qP);
                kbClient.setQuery(q.getQueryString());
    
        
                StringTokenizer st = new StringTokenizer(iri,"_");
                for(int i=0;i<2;i++)
                st.nextToken();
                String schoolName, schoolType, studentClassification;

                schoolName = st.nextToken();
                schoolType = st.nextToken();
                studentClassification = st.nextToken();
    
    
                final String schoolTypeIri = OntoSchool + "School_SchoolType" + UUID.randomUUID();
                final String studentClassificationIri = OntoSchool + "School_StudentClassification" + UUID.randomUUID();
                
                if(schoolType.equalsIgnoreCase("GOVERNMENT"))
                {
                    TriplePattern updatePattern = iri(schoolTypeIri).isA(Government);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());
                } 
                else if(schoolType.equalsIgnoreCase("GOVERNMENTAIDED"))
                {
                    TriplePattern updatePattern = iri(schoolTypeIri).isA(GovernmentAided);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());
                }
                else if(schoolType.equalsIgnoreCase("INDEPENDENT"))
                {
                    TriplePattern updatePattern = iri(schoolTypeIri).isA(Independent);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());
                }
                else if(schoolType.equalsIgnoreCase("SPECIALISEDINDEPENDENT"))
                {
                    TriplePattern updatePattern = iri(schoolTypeIri).isA(SpecialisedIndependent);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());
                }
                else 
                {
                    TriplePattern updatePattern = iri(schoolTypeIri).isA(Specialised);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());

                }

                if(studentClassification.equalsIgnoreCase("BOYS"))
                {
                    TriplePattern updatePattern = iri(studentClassificationIri).isA(Boys);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());
                } 
                else if(studentClassification.equalsIgnoreCase("GIRLS"))
                {
                    TriplePattern updatePattern = iri(studentClassificationIri).isA(Girls);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());
                }
                else 
                {
                    TriplePattern updatePattern = iri(studentClassificationIri).isA(COED);
                    InsertDataQuery insert2 = Queries.INSERT_DATA(updatePattern);
                    insert2.prefix(PREFIX_ONTOSCHOOL);

                    kbClient.executeUpdate(insert2.getQueryString());

                }

                String result=null;
                try
                {
                    Variable schoolIRI = SparqlBuilder.var("schoolIRI");
                    SelectQuery query = Queries.SELECT();
                    TriplePattern queryPattern = schoolIRI.has(hasName,schoolName);

                    query.prefix(PREFIX_ONTOSCHOOL).select(schoolIRI).where(queryPattern);
                    kbClient.setQuery(query.getQueryString());

                    JSONArray queryResult = kbClient.executeQuery();
                    TriplePattern pattern1,pattern2;
    
                    if(!queryResult.isEmpty())
                    {
                        result = kbClient.executeQuery().getJSONObject(0).getString("schoolIRI");
                        pattern1 = iri(result).has(hasSchoolType,iri(schoolTypeIri));
                        pattern2 = iri(result).has(hasStudentClassification,iri(studentClassificationIri));
                    }
                    else
                    {
                        //if queryresult is empty 
                        result = OntoSchool + "School_" + UUID.randomUUID();
                        pattern1 = iri(result).has(hasSchoolType,iri(schoolTypeIri));
                        pattern2 = iri(result).has(hasStudentClassification,iri(studentClassificationIri));
                    }

                    InsertDataQuery insert = Queries.INSERT_DATA(pattern1);
                    insert.prefix(PREFIX_ONTOSCHOOL);
                    kbClient.executeUpdate(insert.getQueryString());

                    InsertDataQuery Insert = Queries.INSERT_DATA(pattern2);
                    Insert.prefix(PREFIX_ONTOSCHOOL);
                    kbClient.executeUpdate(Insert.getQueryString());


                    TriplePattern pattern = iri(result).isA(School);
                    InsertDataQuery insert12 = Queries.INSERT_DATA(pattern);
                    insert12.prefix(PREFIX_ONTOSCHOOL);
                    kbClient.executeUpdate(insert12.getQueryString());



                    //TriplePattern to link schoolType IRI to Data IRI
    
                    TriplePattern updatePattern = iri(schoolTypeIri).has(hasSchoolType,iri);
                    InsertDataQuery insertUpdate = Queries.INSERT_DATA(updatePattern);
                    insertUpdate.prefix(PREFIX_ONTOSCHOOL);
                    kbClient.executeUpdate(insertUpdate.getQueryString());
    
                    //TriplePattern to linke studentClassificationType IRI to Data IRI
    
                    TriplePattern updatePattern1 = iri(studentClassificationIri).has(hasStudentClassification,iri);
                    InsertDataQuery insertUpdate1 = Queries.INSERT_DATA(updatePattern1);
                    insertUpdate1.prefix(PREFIX_ONTOSCHOOL);
                    kbClient.executeUpdate(insertUpdate1.getQueryString());    

                }
                catch(Exception e)
                {
                    throw new JPSRuntimeException("Unable to execute query: " + q.getQueryString());
                }
    
            

                String result1=null;
                Variable schoolAddressIRI = SparqlBuilder.var("schoolAddressIRI");
                SelectQuery query1 = Queries.SELECT();

                TriplePattern queryPattern1 = iri(result).has(hasAddress,schoolAddressIRI);
                query1.prefix(PREFIX_ONTOSCHOOL).select(schoolAddressIRI).where(queryPattern1);
                kbClient.setQuery(query1.getQueryString());


                try
                {
                    JSONArray queryResult1 = kbClient.executeQuery();

                    if(!queryResult1.isEmpty())
                    {
                        //School IRI and its following relations have already been instantiated. Loop proceeds to the next IRI immediately as it doesnt enter the else condition.
                    }
                    else
                    {
                        String build2 = OntoSchool + "SchoolAddress_" + UUID.randomUUID();
                        TriplePattern pattern2 = iri(result).has(hasAddress,build2);
                        InsertDataQuery insert2 = Queries.INSERT_DATA(pattern2);
                        insert2.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert2.getQueryString());

                        //Obtaining Address
                        TriplePattern pattern3 = iri(build2).isA(Address);
                        InsertDataQuery insert3 = Queries.INSERT_DATA(pattern3);
                        insert3.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert3.getQueryString());

                        String add="",email="",postal="",tele="";
                        try
                        {
                           JSONArray jsArr;
                           jsArr = schoolReadings.getJSONObject("result").getJSONArray("records");
                           for(int i=0; i<jsArr.length();i++)
                           {
                              JSONObject currentSchool = jsArr.getJSONObject(i);
                              String name = currentSchool.getString("school_name");

                              if(name.equalsIgnoreCase(schoolName))
                              {
                                add = currentSchool.getString("address");
                                email = currentSchool.getString("email_address");
                                tele = currentSchool.getString("telephone_no");
                                postal = currentSchool.getString("postal_code");
                                
                                i = jsArr.length();
                              }
                           }
                        }
                        catch(Exception e )
                        {
                            throw new JPSRuntimeException("Readings can not be empty!", e);
                        }

                        //TriplePattern for address
                        TriplePattern pattern4 = iri(result).has(hasAddress,add);
                        InsertDataQuery insert4 = Queries.INSERT_DATA(pattern4);
                        insert4.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert4.getQueryString());

                        //TriplePattern for email_address
                        TriplePattern pattern5 = iri(result).has(hasEmailAddress,email);
                        InsertDataQuery insert5 = Queries.INSERT_DATA(pattern5);
                        insert5.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert5.getQueryString());

                        //TriplePattern for postal code
                        TriplePattern pattern6 = iri(result).has(hasPostalCode,postal);
                        InsertDataQuery insert6 = Queries.INSERT_DATA(pattern6);
                        insert6.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert6.getQueryString());

                        //TriplePattern for telephone Numbder 
                        TriplePattern pattern7 = iri(result).has(hasTelephoneNumber,tele);
                        InsertDataQuery insert7 = Queries.INSERT_DATA(pattern7);
                        insert7.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert7.getQueryString());


                        //FuzzyMatching for the MOEProgrammes;
                        String prog="";

                        try
                        {
                            JSONArray jsArr = programmeReadings.getJSONObject("result").getJSONArray("records");
                            for(int i=0; i<jsArr.length();i++)
                            {
                                JSONObject currentSchool = jsArr.getJSONObject(i);
                                String current = currentSchool.getString("school_name");

                                if(FuzzySearch.tokenSetRatio(current.toLowerCase(),schoolName.toLowerCase())>90 &&
                                FuzzySearch.partialRatio(current.toLowerCase(),schoolName.toLowerCase())>75 && FuzzySearch.tokenSortRatio(current.toLowerCase(),schoolName.toLowerCase())>83)
                                {
                                    prog = currentSchool.getString("moe_programme_desc");
                                    i=jsArr.length();
                                }

                            }

                        }
                        catch(Exception e)
                        {
                            throw new JPSRuntimeException("Readings can not be empty!", e);
                        }

                        TriplePattern pattern8 = iri(result).has(hasProgramme,prog);
                        InsertDataQuery insert8 = Queries.INSERT_DATA(pattern8);
                        insert8.prefix(PREFIX_ONTOSCHOOL);
                        kbClient.executeUpdate(insert8.getQueryString());

                        //FuzzyMatching for the Schools CCA

                        try
                        {
                            JSONArray jsArr = ccaReadings.getJSONObject("result").getJSONArray("records");
                            for(int i=0; i<jsArr.length();i++)
                            {
                                JSONObject currentSchool = jsArr.getJSONObject(i);
                                String current = currentSchool.getString("school_name");

                                if(FuzzySearch.tokenSetRatio(current.toLowerCase(),schoolName.toLowerCase())>90 &&
                                FuzzySearch.partialRatio(current.toLowerCase(),schoolName.toLowerCase())>75 && FuzzySearch.tokenSortRatio(current.toLowerCase(),schoolName.toLowerCase())>83)
                               {
                                    String ccaGroup = currentSchool.getString("cca_grouping_desc");
                                    String ccaName = currentSchool.getString("cca_generic_name");

                                    TriplePattern pattern9 = iri(result).has(hasCCAGroup,ccaGroup);
                                    InsertDataQuery insert9 = Queries.INSERT_DATA(pattern9);
                                    insert9.prefix(PREFIX_ONTOSCHOOL);
                                    kbClient.executeUpdate(insert9.getQueryString());

                                    TriplePattern pattern10 = iri(result).has(hasCCA,ccaName);
                                    InsertDataQuery insert10 = Queries.INSERT_DATA(pattern10);
                                    insert10.prefix(PREFIX_ONTOSCHOOL);
                                    kbClient.executeUpdate(insert10.getQueryString());

                                }
                            }

                        }
                        
                        catch(Exception e)
                        {
                            throw new JPSRuntimeException("Readings can not be empty!", e);
                        }
                    }

                }
                catch(Exception e)
                {
                    throw new JPSRuntimeException("Unable to execute query: " +  query1.getQueryString());
                }
               //Looping through for subsequent IRIs
            }
            
                
        }
           
    }
}

