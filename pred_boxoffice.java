//STEP 1. Import required packages
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;

public class pred_boxoffice  {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://54.86.203.127/TRAIN";

   //  Database credentials
   static final String USER = "bill";
   static final String PASS = "passpass";
   
   public static void update_predBoxOffice()
   {
	   Connection conn = null;
	   Statement stmt = null;
	   //list to store movie names, will be used to process pred box office collection
	   //for each movie iteratively.
	   ArrayList<String> movielist = new ArrayList<String>();
	   try{
	      Class.forName("com.mysql.jdbc.Driver");
	      System.out.println("Connecting to a selected database...");
	      conn = DriverManager.getConnection(DB_URL, USER, PASS);
	      System.out.println("Connected database successfully");
	      stmt = conn.createStatement();
	      //get a list of movies stored in the database
	      String sql = "SELECT movie_name FROM augur_test2";
	      ResultSet movies = stmt.executeQuery(sql);
	      while(movies.next())
	      {
	         //Retrieve by column name
	         String movie_name = movies.getString("movie_name");
	         movielist.add(movie_name);
	         
	      }
	      movies.close();
	      //get maximum views,max like_dislike,max_collection from database
	      String getMaxViews = "select MAX( YT_views) from augur_train2";
	      String getMaxLikes_dislikes = "select MAX(like_dislike) from augur_train2";
	      String getMaxCollection = "select MAX(collection) from augur_train2";
	      float MaxViews = stmt.executeQuery(getMaxViews).getFloat(0);
	      float Maxdelta_likes = stmt.executeQuery(getMaxLikes_dislikes).getFloat(0);
	      float MaxCollection = stmt.executeQuery(getMaxCollection).getFloat(0);
	      //for each movie in database
	      for (int i =0;i<movielist.size();i++)
	      {
	    	  //get views
	    	  String getViews = "select YT_views from augur_test2 where movie_name="+movielist.get(i);
	    	  float views = stmt.executeQuery(getViews).getFloat(0);
	    	  //get delta_likes
	    	  String getDeltaLike = "select like_dislike from augur_test2 where movie_name="+movielist.get(i);
	    	  float delta_likes = stmt.executeQuery(getDeltaLike).getFloat(0);
	    	  //calculate hype factor
	    	  float HF = (views/MaxViews)+(delta_likes/Maxdelta_likes);
	    	  //get sentiment factor
	    	  String getSF =  "select var_coll from augur_test2 where movie_name="+movielist.get(i);
	    	  String SF = stmt.executeQuery(getSF).getString(0);
	    	  float SFmin=0,SFmax=0;
	    	//depending on SF set SFmax,SFmin
	    	  switch(SF)
	    	  {
	    	  case "A":
	    		  SFmin=60000000;
	    		  SFmax=MaxCollection;
	    	  case "B":
	    		  SFmin=10000000;
	    		  SFmax=60000000;
	    	  case "C":
	    		  SFmin=1000000;
	    		  SFmax=10000000;
	    	  case "D":
	    		  SFmin=300000;
	    		  SFmax=1000000;
	    	  case "E":
	    		  SFmin=100000;
	    		  SFmax=300000;
	    	  case "F":
	    		  SFmin=0;
	    		  SFmax=100000;
	    	  }
	    	  //calculate collection
	    	  float pred_BO = SFmin + (HF/2)*(SFmax-SFmin);
	    	  //update table
	    	  String update_predBO = "UPDATE augurdata SET pred_collection = ? WHERE movie_name = ?";
	    	  PreparedStatement ps = conn.prepareStatement(update_predBO);
	    	  ps.setDouble(1,pred_BO);
	    	  ps.setString(2, movielist.get(i));
	    	  ps.executeUpdate();
	    	  System.out.println("pred collection updated for "+ movielist.get(i));
	      }
	   }catch(SQLException se){
	      //Handle errors for JDBC
	      se.printStackTrace();
	   }catch(Exception e){
	      //Handle errors for Class.forName
	      e.printStackTrace();
	   }finally{
	      //finally block used to close resources
	      try{
	         if(stmt!=null)
	            conn.close();
	      }catch(SQLException se){
	      }// do nothing
	      try{
	         if(conn!=null)
	            conn.close();
	      }catch(SQLException se){
	         se.printStackTrace();
	      }//end finally try
	   }//end try
   }   
   public  static void main(String[] args) throws Exception
   {
	   update_predBoxOffice();
   }//end main
}//end FirstExample
