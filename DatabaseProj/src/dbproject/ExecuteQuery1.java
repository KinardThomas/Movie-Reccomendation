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


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.SwingUtilities;
import java.sql.*;

/**
 *
 * 
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
    
    public String retrieveTopMovieValues(){
        String sql = "";
        String msg = "";
        PreparedStatement pstmt = null;
        
        try{
            
            sql = "Select title, yearProduced, rtAudienceRating, "
                    + "rtPictureURL, imdbPictureURL FROM spicymov.movies " +
                    "GROUP BY title " +
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
                msg += yearProduced + "\t" + audienceRating + "\t" + title +
                        "\t" + rtPictureURL + "\t" + imdbPictureURL + "\n";
            }
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    return msg;
    }
    public String retrieveDirectors(String requestedDirector){
        String sql = "";
        PreparedStatement pstmt = null;
        String msg = "";
        try{
            sql = "SELECT title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    " FROM spicymov.movies AS M " +
                    " WHERE M.id IN (Select movieID FROM spicymov.directors_table AS DT WHERE DT.directorName = " +
                    "\"" + requestedDirector + "\"" + ") " +
                    "GROUP BY title; ";
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
                msg +=   yearProduced + "\t" + audienceRating + "\t" + title +
                        "\t" + rtPictureURL + "\t" + imdbPictureURL + "\n";     
            }
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        return msg;
        
    }
    public void retrieveActors(){
        String sql = "";
        PreparedStatement pstmt = null;
        
        try{
            
            sql = "SELECT	actorName\n" +
"	FROM		Actors A\n" +
"	WHERE 	rownum <=20 (SELECT a.actor FROM movies m AND actors a WHERE a.movieID = 	m.movieID ORDER BY m.rtAudienceScore DESC);";
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
    public String retrieveMatchingTags(String requestedMovieName){
        String sql1 = "";
        String sql2 = "";
        
        String msg = "";
        PreparedStatement pstmt = null;
        PreparedStatement pst;
                
        try{
            
            sql2 = "Select title, yearProduced, rtAudienceRating, " +
                    "rtPictureURL, imdbPictureURL FROM spicymov.movies " +
                    "WHERE title = " + "\"" + requestedMovieName + "\"" +
                    "GROUP BY title;";
            pst = test.con.prepareStatement(sql2);
            
            ResultSet rs1 = pst.executeQuery();
            while(rs1.next()){
                msg += rs1.getString("title") + "\t" + rs1.getInt("yearProduced") +
                        "\t" + rs1.getDouble("rtAudienceRating") + "\t" +
                        rs1.getString("rtPictureURL") + "\t" + rs1.getString("imdbPictureURL");
            }
            sql1 = "Select DISTINCT tagText " +
                    "FROM spicymov.movie_tagText AS T, spicymov.movies AS M, spicymov.movie_tag_identification AS TID " +
                    "WHERE M.title = "+ "\"" +requestedMovieName + "\"" +  " AND TID.movie_ID = M.id AND TID.tagID = T.tagID;";
            pstmt = test.con.prepareStatement(sql1);
            try (ResultSet rs = pstmt.executeQuery()) {
                while(rs.next()){
                    msg += rs.getString("tagText") + "\n";
                    
                }
                System.out.println(msg);
            }
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
                
        return msg;
    }
    
    
    public String retrieveMatchingGenres(String requestedGenre){
        String sql = "";
        String msg = "";
        PreparedStatement pstmt = null;
        
        try{

            sql = "Select DISTINCT title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    "FROM spicymov.movie_genres AS MG, spicymov.movies AS M " +
                    "WHERE MG.genre = "+ "\"" +requestedGenre + "\"" +  " AND M.id = MG.movieID " +
                    "GROUP BY title";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            
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
        return msg;
    }
    public String retrieveMatching20genres(String requestedGenre){
        String sql = "";
        String msg = "";
        PreparedStatement pstmt = null;
        
        try{

            sql = "Select DISTINCT title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    "FROM spicymov.movie_genres AS MG, spicymov.movies AS M " +
                    "WHERE MG.genre = "+ "\"" +requestedGenre + "\"" +  " AND M.id = MG.movieID " +
                    "ORDER BY rtAudienceRating DESC " +
                    "LIMIT 20;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            
            while(rs.next()){
                msg += rs.getString("title") + "\t" + rs.getInt("yearProduced") + 
                        "\t"+ rs.getDouble("rtAudienceRating") + "\t" + rs.getString("rtPictureURL")
                        + "\t" + rs.getString("imdbPictureURL")+ "\n";
                
                
            }
            System.out.println(msg);
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        return msg;
    }
    public String retrieveDirectedMovies(String requestedGenre){
        String sql = "";
        PreparedStatement pstmt = null;
        String msg = "";
        try{

            sql = "Select DISTINCT title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    "FROM spicymov.movie_genres AS MG, spicymov.movies AS M " +
                    "WHERE MG.genre = "+ "\"" +requestedGenre + "\"" +  " AND M.id = MG.movieID;" +
                    "ORDER BY rtAudienceRating DESC " +
                    "LIMIT 20;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
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
        return msg;
    }
    public String retrieveTopMovieDirectors(){
        String sql = "";
        PreparedStatement pstmt = null;
        String msg = "";
        try{
            
            sql = "SELECT  directorName, AVG(rtAudienceRating) " + 
"FROM spicymov.movies AS M, spicymov.directors_table AS A " +
"WHERE M.id = A.movieID " +
"GROUP BY directorName " +
"ORDER BY AVG(rtAudienceRating) DESC " +
"LIMIT 20;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            
            while(rs.next()){
                String directorName = rs.getString("directorName");
                Double sum = rs.getDouble("AVG(rtAudienceRating)");
                msg += directorName + "\t" + sum + "\n";
                
                       
            }
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        return msg;
    }
    public String retrieveTopMovieActors(){
        String sql = "";
        PreparedStatement pstmt = null;
        String msg = "";
        try{
            
            sql = "SELECT  actorName, AVG(rtAudienceRating) " +
"FROM spicymov.movies AS M, spicymov.actors_table AS A " +
"WHERE M.id = A.movieID " +
"GROUP BY actorName " +
"ORDER BY AVG(rtAudienceRating) DESC " +
"LIMIT 20;";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            
            while(rs.next()){
                double audienceRating = rs.getDouble("AVG(rtAudienceRating)");
                String actorName = rs.getString("actorName");
                msg += audienceRating + "\t" + actorName + "\n";
                       
            }
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        return msg;
    }
      public String retrieveActorInMovies(String requestedActor){
        String sql = "";
        PreparedStatement pstmt = null;
        String msg = "";
        try{

            sql = "Select title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    "FROM  spicymov.movies AS M " +
                    "WHERE M.id IN ( "+ 
                    "SELECT A.movieID " +
                    "FROM spicymov.actors_table AS A " +
                    "WHERE A.actorName = " + "\"" + requestedActor + "\"" + ") " +
                    "GROUP BY title";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
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
        return msg;
    }
  public String retrieveAnyTag(String requestedtag){
        String sql = "";
        PreparedStatement pstmt = null;
        String msg = "";
        try{

            sql = "Select title, yearProduced, rtAudienceRating, rtPictureURL, imdbPictureURL " +
                    "FROM spicymov.movies AS M " +
                    "WHERE M.id IN (SELECT movie_ID " +
                    "FROM spicymov.movie_tagtext AS T, spicymov.movie_tag_identification AS TI " +
                    "WHERE T.tagText = " + "\"" + requestedtag + "\"" +
                    ") GROUP BY title";
            pstmt = test.con.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                msg += rs.getString("title") + "\t" + rs.getInt("yearProduced") + 
                        "\t"+ rs.getDouble("rtAudienceRating") + "\t" + rs.getString("rtPictureURL")
                        + "\t" + rs.getString("imdbPictureURL")+ "\n";         
            }
            System.out.println(msg);
            
            rs.close();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        return msg;
    }

    public void closeConnection(){
        try {
            test.con.close();
        } catch (SQLException ex) {
            System.out.println(ex.toString());
        }
    }
}

