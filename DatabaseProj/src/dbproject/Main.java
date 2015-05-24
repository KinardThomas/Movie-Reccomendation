/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbproject;
import java.io.*;
import java.sql.*;
import java.util.*;
import javax.swing.*;
/**
 *
 * @author jmac
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
            
        
            
       Scanner input = new Scanner(System.in);
        
        
        try{
            
            
            insertAllValuesToDB insertQuery = new insertAllValuesToDB();
            insertQuery.insertMovieValuesIntoDB("movies.dat");
            insertQuery.insertUserTags();
            insertQuery.insertGenres();
            insertQuery.insertDirectors();
            insertQuery.insertActors();
            insertQuery.insertMovieIdentification();
            //insertQuery.checkDuplicates();
            /*ExecuteQuery1 query1 = new ExecuteQuery1();
            query1.retrieveTopMovieValues();
            System.out.println("Please enter a movie to find it's tags: ");
            query1.retrieveMatchingTags(input.nextLine());
            System.out.println("Please enter a genre to find related movies: ");
            query1.retrieveMatchingGenres(input.nextLine());*/
            gui temp = new gui();
            temp.setVisible(true);
            
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
        
        /*String s = "Hello" + "\t" + "\N";
        String[] testArray = s.split("\t");
        System.out.println(testArray[1].);*/
        /*dbConnection test;
        try{
            test = new dbConnection();
        }
        catch(Exception e){
            System.out.println(e.toString());
        }*/
        
    }
    
}
