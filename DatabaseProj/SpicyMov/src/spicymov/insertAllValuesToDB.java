/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package spicymov;

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
        int count = 0;
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
                
                
                stm.setInt(1, Integer.parseInt(lineArray[0]));
                
                stm.setString(2, lineArray[1].replaceAll("'", "\""));
                
                
                if(lineArray[2].isEmpty()){
                    stm.setNull(3, Types.NULL);
                }
                else{
                    stm.setInt(3, Integer.parseInt(lineArray[2]));
                }
                
               
                if(lineArray[4].isEmpty() || (lineArray[4].equals( "\\N"))){
                    stm.setNull(4, Types.NULL);
                }
                else{
                     String imdbURL = lineArray[4].replaceAll("'", "\"");
                    stm.setString(4, imdbURL);
                }
                
                stm.setInt(5, Integer.parseInt(lineArray[5]));
                
                if(lineArray[20].isEmpty() || (lineArray[20].equals("\\N"))){
                    stm.setNull(6, Types.NULL);
                    
                }
                else{
                    String rtURL = lineArray[20].replaceAll("'", "\"");
                    stm.setString(6, rtURL);
                }
                
                try{
                    stm.setDouble(7, Double.parseDouble(lineArray[17]));
                    stm.addBatch();
                }
                catch(Exception E){
                    stm.setNull(7, Types.NULL);
                    stm.addBatch();
                }

                
                
                count++;
            if(count==1000){
                count=0;
                
            }    
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
        int count = 0;
        PreparedStatement stm = null;
        try{
            file = new File("tags.dat");
            f = new FileInputStream(file);
            ir = new InputStreamReader(f, "ISO-8859-1");
            reader = new BufferedReader(ir);
            stm = test.con.prepareStatement(sql);
            
            File file2 = new File("user_taggedmovies-timestamps.dat");
                FileInputStream f2 = new FileInputStream(file2);
                InputStreamReader r2 = new InputStreamReader(f2, "ISO-8859-1");
                BufferedReader reader2 = new BufferedReader(r2);
                reader2.readLine();
            
            
            
            long start = System.currentTimeMillis();
            
            String line;
            String[] lineArray;
            
            String tagString;
            String tagStringArray[];
            
            try{
            
            reader.readLine();
            
            while((line = reader.readLine()) != null){
                lineArray = line.split("[\t]");
                stm.setInt(1, Integer.parseInt(lineArray[0]));
                stm.setString(2, (lineArray[1]).replaceAll("'","\""));
                
                
             
                stm.addBatch();
                
                System.out.println(count);
                count++;
            }
            stm.executeBatch();
            
            
              
             
                
            sql = "INSERT INTO spicyMov.movie_tag_identification(movieID, tagID, tstamp) " + "" 
                 + "values (?,?,?)";
            stm = test.con.prepareStatement(sql);
            while((tagString = reader2.readLine()) != null){
                tagStringArray= tagString.split("[\t]");
                stm.setInt(1, Integer.parseInt(tagStringArray[1]));
                stm.setInt(2, Integer.parseInt(tagStringArray[2]));
                stm.setFloat(3, Float.parseFloat(tagStringArray[3]));
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
    
    public void insertGenres(){
        String sql = "INSERT INTO spicymov.movie_genres(movieID, genre) " + "values(?,?)";
        PreparedStatement pst1;
        
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
                
                pst1.setInt(1, Integer.parseInt(lineStringArray[0]));
                
                pst1.setString(2, lineStringArray[1].replaceAll("'", "\""));
                
                
                
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
    
    }
    
