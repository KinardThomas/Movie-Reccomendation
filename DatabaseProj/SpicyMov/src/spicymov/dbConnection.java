/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package spicymov;
import java.io.*;
import java.sql.*;

/**
 *
 * @author jmac
 */
public class dbConnection {
    String msg;
    
    Connection con;
    Statement stmt;
    ResultSet rs;
    public dbConnection() throws ClassNotFoundException, SQLException{
        try{
            Class.forName("com.mysql.jdbc.Driver");
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/spicymov?rewriteBatchedStatements=true", "root", "root");
            System.out.println("Connection successful");
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        
        
    }
    public void getTxt(){
        System.out.println(msg);
    }
    public Connection getConnection(){
        return con;
    }
    
    
    
    
    
    public void closeConnection(){
        try{
          con.close();  
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
    }

}
