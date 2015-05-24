/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbproject;

/**
 *
 * @author Kinard
 */
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;

/**
 *
 * @author Jmac
 */
public class insertAllValuesToDB {
    
    dbConnection test;
    Statement stmt;
    ResultSet rs;
    
    File file = null;
    FileInputStream f;
    BufferedReader reader = null;
    InputStreamReader ir;
    
    HashSet<Integer> insertedDirectors = new HashSet<Integer>();
    
    
    public insertAllValuesToDB(){
        try{
           test = new dbConnection(); 
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    
    public void insertMovieValuesIntoDB(String filename){
        
        String sql = "INSERT INTO spicyMov.movies(id, title, imdbID, imdbPictureURL, yearProduced" +
                ", rtPictureURL, rtAudienceRating) " + "values (?,?,?,?,?,?,?)";
        PreparedStatement stm = null;
        try{
            file = new File("movies.dat");
            f = new FileInputStream(file);
            ir = new InputStreamReader(f, "ISO-8859-1");
            reader = new BufferedReader(ir);
            stm = test.con.prepareStatement(sql);
            
            long start = System.currentTimeMillis();
            
            String line;
            String[] lineArray;
            
            reader.readLine();
            
            
            while((line = reader.readLine()) != null){
                lineArray = line.split("[\t]");
                System.out.println(lineArray[0] + " " + lineArray[1] + " " + lineArray[17]);
                
                try{
                    stm.setInt(1, Integer.parseInt(lineArray[0])); //set value for id.
                }
                catch (Exception e){
                    continue;
                }
                
                if(lineArray[1].isEmpty() || (lineArray[1].equals("\\N"))){
                   stm.setNull(2, Types.NULL); // set title for statement 
                }
                else{
                    stm.setString(2, lineArray[1].replaceAll("'", "\""));
                }
                
                
                if(lineArray[2].isEmpty() || (lineArray[2].equals("\\N"))){ //Check to see if the imdbID is empty
                                                                            //And/or if it is null
                    stm.setNull(3, Types.NULL);
                }
                else{
                    stm.setInt(3, Integer.parseInt(lineArray[2])); //Add it otherwise.
                }
                
               
                if(lineArray[4].isEmpty() || (lineArray[4].equals( "\\N"))){ //imdbPictureURL (same as for imdbID)
                    stm.setNull(4, Types.NULL);
                }
                else{
                     String imdbURL = lineArray[4].replaceAll("'", "\"");
                    stm.setString(4, imdbURL);
                }
                try{
                    stm.setInt(5, Integer.parseInt(lineArray[5])); //insert year produced
                }
                catch(Exception e){
                    stm.setNull(5, Types.NULL);
                }
                
                if(lineArray[20].isEmpty() || (lineArray[20].equals("\\N"))){ //insert rtPictureURL
                    stm.setNull(6, Types.NULL);
                    
                }
                else{
                    String rtURL = lineArray[20].replaceAll("'", "\"");
                    stm.setString(6, rtURL);
                }
                
                try{
                    stm.setDouble(7, Double.parseDouble(lineArray[17])); //rtAudienceRating 
                    stm.addBatch();
                }
                catch(Exception E){
                    stm.setNull(7, Types.NULL);
                    stm.addBatch();
                }
                
                insertedDirectors.add(Integer.parseInt(lineArray[0]));
                

                
                
                  
            }
            
            
            stm.executeBatch();
            
            System.out.println("Time Take=" +(System.currentTimeMillis()-start));
            stm.close();
        }
            
        catch(FileNotFoundException x){
            System.out.println("File not found, Please enter correct file name and try again.");
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
        try{
        reader.close();
        }
        catch(IOException e){
            System.out.println(e.toString());
        }
    }
    
    public void insertUserTags(){
        Set<String> uniqueStrings = new HashSet<String>();
        String sql = "INSERT INTO spicyMov.movie_tagText(tagID, tagText) " + "" 
                 + "values (?,?)";
        PreparedStatement stm = null;
        try{
            file = new File("tags.dat");
            f = new FileInputStream(file);
            ir = new InputStreamReader(f, "ISO-8859-1");
            reader = new BufferedReader(ir);
            stm = test.con.prepareStatement(sql);
            
            
            
            
            
            long start = System.currentTimeMillis();
            
            String line;
            String[] lineArray;
            
            try{
            
            reader.readLine();
            
            while((line = reader.readLine()) != null){
                lineArray = line.split("[\t]");
                try{
                stm.setInt(1, Integer.parseInt(lineArray[0])); //tagID
                stm.setString(2, (lineArray[1]).replaceAll("'","\"")); //tagText
                }
                catch(Exception e){
                    stm.clearParameters(); //No need to store the value if no title exists and vice-versa.
                    continue;
                }
                
             
                stm.addBatch();
                
            }
            stm.executeBatch();
            
            

            }
            catch(IOException e){
                    System.out.println(e.toString());
                    }    
                
            }
        
            
        catch(FileNotFoundException x){
            System.out.println("File not found, Please enter correct file name and try again.");
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
        
        try{
        reader.close();
        }
        catch(IOException e){
            System.out.println(e.toString());
        }
    }
    
    public void insertMovieIdentification(){
        HashSet<Tag> insertedIDs = new HashSet<Tag>();
        PreparedStatement stm;
        String sql = "INSERT INTO spicyMov.movie_tag_identification(movie_ID, tagID, tstamp, userID) " + "" 
                 + "values (?,?,?,?)";
        String tagString = "";
        String tagStringArray[];
        try{
            File file2 = new File("user_taggedmovies-timestamps.dat");
                FileInputStream f2 = new FileInputStream(file2);
                InputStreamReader r2 = new InputStreamReader(f2, "ISO-8859-1");
                BufferedReader reader2 = new BufferedReader(r2);
                reader2.readLine();
            stm = test.con.prepareStatement(sql);
            while((tagString = reader2.readLine()) != null){
                tagStringArray= tagString.split("[\t]");
                
                try{
                stm.setInt(1, Integer.parseInt(tagStringArray[1]));
                stm.setInt(2, Integer.parseInt(tagStringArray[2]));
                stm.setFloat(3, Float.parseFloat(tagStringArray[3]));
                stm.setInt(4, Integer.parseInt(tagStringArray[0]));
                }
                catch(Exception e){
                    stm.clearParameters();
                    continue;
                }
                stm.addBatch();
                
            } 
            stm.executeBatch();
        }
        
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    public void insertGenres(){
        String sql = "INSERT INTO spicymov.movie_genres(movieID, genre) " + "values(?,?)";
        PreparedStatement pst1;
        Integer count = 0;
        try{
            file = new File("movie_genres.dat");
            f = new FileInputStream(file);
            ir = new InputStreamReader(f, "ISO-8859-1");
            reader = new BufferedReader(ir);
            pst1 = test.con.prepareStatement(sql);
            
            String lineString;
            String[] lineStringArray;
            
            reader.readLine();
            
            while((lineString = reader.readLine()) != null){
                lineStringArray = lineString.split("[\t]"); 
                
                try{
                
                pst1.setInt(1, Integer.parseInt(lineStringArray[0])); //movieID
                
                pst1.setString(2, lineStringArray[1].replaceAll("'", "\"")); //genre
                
                }
                catch(Exception e){
                    pst1.clearParameters();
                    continue;
                }
                
                
                pst1.addBatch();
                count++;
            }
            pst1.executeBatch();
        }
        catch(FileNotFoundException fnf1){
            System.out.println(fnf1.toString());
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
    }
    
    public void insertDirectors(){
        String sql = "INSERT INTO spicymov.directors_table(movieID, director_ID, directorName) " + "values(?,?,?)";
        PreparedStatement pst1;
        
        try{
            file = new File("movie_directors.dat");
            f = new FileInputStream(file);
            ir = new InputStreamReader(f, "ISO-8859-1");
            reader = new BufferedReader(ir);
            pst1 = test.con.prepareStatement(sql);
            

            
            
            String lineString;
            String[] lineStringArray;
            
            reader.readLine();
            
            while((lineString = reader.readLine()) != null){
                lineStringArray = lineString.split("[\t]"); 
                if(lineStringArray[1].isEmpty() || lineStringArray[1].equals("\\N")){ // check to see if director id is empty.
                    continue;
                }
                else{
                    pst1.setInt(1, Integer.parseInt(lineStringArray[0]));
                    pst1.setString(2, lineStringArray[1]);
                    pst1.setString(3, lineStringArray[2]);
                }
               
                
                
                
                pst1.addBatch();
            }
            pst1.executeBatch();
        }
        catch(FileNotFoundException fnf1){
            System.out.println(fnf1.toString());
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    
    public void insertActors(){
        String sql = "INSERT INTO spicymov.actors_table(movieID, actorID, actorName, ranking) " + "values(?,?,?,?)";
        PreparedStatement pst1;
        
        try{
            file = new File("movie_actors.dat");
            f = new FileInputStream(file);
            ir = new InputStreamReader(f, "ISO-8859-1");
            reader = new BufferedReader(ir);
            pst1 = test.con.prepareStatement(sql);
            
            String lineString;
            String[] lineStringArray;
            
            reader.readLine();
            
            while((lineString = reader.readLine()) != null){
                lineStringArray = lineString.split("[\t]"); 
                if(lineStringArray[1].isEmpty() || lineStringArray[1].equals("\\N")){ // check to see if actor id is empty.
                    continue;
                }
                else{
                    pst1.setInt(1, Integer.parseInt(lineStringArray[0]));
                    pst1.setString(2, lineStringArray[1]);
                    pst1.setString(3, lineStringArray[2]);
                    pst1.setInt(4, Integer.parseInt(lineStringArray[3]));
                }
                
                
                
                pst1.addBatch();
            }
            pst1.executeBatch();
        }
        catch(FileNotFoundException fnf1){
            System.out.println(fnf1.toString());
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    
    public void checkDuplicates(){
        HashSet<Integer> set1;
        set1 = new HashSet<Integer>();
        
        
        String sqlCreateTempTable = "create table spicymov.movie_duplicates AS " +
"SELECT id, imdbID " +
"FROM spicymov.movies " +
"where id not in( " +
"Select MAX(id) " +
"FROM spicymov.movies " +
"GROUP BY title " +
"HAVING COUNT(title) > 1)";
        
        try{
            Statement temp = test.con.createStatement();
            temp.executeUpdate(sqlCreateTempTable);
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
        String sql = "DELETE FROM spicymov.movies  where id in( " +
"Select id " +
"FROM spicymov.movie_duplicates)";
        Statement pst;
        ResultSet rs;
        try{
        pst = test.con.createStatement();
        pst.executeUpdate(sql);
        
        
        }
        catch (Exception e){
            System.out.println(e.toString());
        }
        //System.out.println(set1.toString());
        
        
        /*String sql1 = "DELETE FROM spicymov.movies " +
                "WHERE id = ?";
        
        try{
            pst1= test.con.prepareStatement(sql1);
            for(Integer x: set1){
                int temp = (Integer) x;
                pst1.setInt(1, temp);
                pst1.addBatch();
            }
            pst1.executeBatch();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }*/
        
    }
    
    }
    

