/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package spicymov;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;


/**
 *
 * @author jmac
 */
public class ExecuteQuery1 {
    dbConnection test;
    
    public ExecuteQuery1(){
        try{
            test = new dbConnection();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    
    public void retrieveTopMovieValues(){
        String sql = "";
        PreparedStatement pstmt = null;
        
        try{
            
            sql = "Select title, yearProduced, rtAudienceRating, "
                    + "rtPictureURL, imdbPictureURL FROM spicymov.movies " +
                    "ORDER BY rtAudienceRating DESC " +
                    "LIMIT 20;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            
            while(rs.next()){
                double audienceRating = rs.getDouble("rtAudienceRating");
                String title = rs.getString("title");
                int yearProduced = rs.getInt("yearProduced");
                String rtPictureURL = rs.getString("rtPictureURL");
                String imdbPictureURL = rs.getString("imdbPictureURL");
                System.out.println(yearProduced + " " + audienceRating + " " + title +
                        " " + rtPictureURL + " " + imdbPictureURL);
                       
            }
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
    }
    public void retrieveMatchingTags(String requestedMovieName){
        String sql = "";
        PreparedStatement pstmt = null;
        
        try{

            sql = "Select DISTINCT tagText " +
                    "FROM spicymov.movie_tagText AS T, spicymov.movies AS M, spicymov.movie_tag_identification AS TID " +
                    "WHERE M.title = "+ "\"" +requestedMovieName + "\"" +  " AND TID.movieID = M.id AND TID.tagID = T.tagID;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            String msg = "";
            while(rs.next()){
                msg += rs.getString("tagText") + "\n";
                
            }
            System.out.println(msg);
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    
    public void retrieveMatchingGenres(String requestedGenre){
        String sql = "";
        PreparedStatement pstmt = null;
        
        try{

            sql = "Select DISTINCT title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    "FROM spicymov.movie_genres AS MG, spicymov.movies AS M " +
                    "WHERE MG.genre = "+ "\"" +requestedGenre + "\"" +  " AND M.id = MG.movieID;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            String msg = "";
            while(rs.next()){
                msg += rs.getString("title") + " " + rs.getInt("yearProduced") + 
                        " "+ rs.getDouble("rtAudienceRating") + " " + rs.getString("rtPictureURL")
                        + " " + rs.getString("imdbPictureURL")+ "\n";
                
                
            }
            System.out.println(msg);
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }
    public void closeConnection(){
        try {
            test.con.close();
        } catch (SQLException ex) {
            System.out.println(ex.toString());
        }
    }
}
