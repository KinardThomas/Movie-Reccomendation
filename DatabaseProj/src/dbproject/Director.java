/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dbproject;

/**
 *
 * @author jmac
 */
public class Director {
    String director_ID;
    int movie_ID;
    String director_name;
    public Director(String d_ID, int mov_ID, String d_name){
        director_ID = d_ID;
        movie_ID = mov_ID;
        director_name = d_name;
    }
}
