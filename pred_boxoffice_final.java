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
   
   public static void update_predBoxOffice(String movie_name)
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
	      PreparedStatement preparedStatement = null;
	      //get a list of movies stored in the database
	      String sql = "SELECT movie_name FROM augur_test2";
	      ResultSet movies = stmt.executeQuery(sql);
	      movielist.add(movie_name);//for demo,remove otherwise.
//	      while(movies.next())
//	      {
//	         //Retrieve by column name
//	         String movie_name = movies.getString("movie_name");
//	         movielist.add(movie_name);
//	         
//	      }
	      movies.close();
	      //get maximum views,max like_dislike,max_collection from database
	      String getMaxViews = "select MAX( YT_views) as maxviews from augur_train2";
	      String getMaxLikes_dislikes = "select MAX(like_dislike) as deltalike from augur_train2";
	      String getMaxCollection = "select MAX(collection) as maxcoll from augur_train2";
	      String getMinCollection = "select MIN(collection) as mincoll from augur_train2";
	      float MaxViews = 0;
	      float Maxdelta_likes = 0;
	      float MaxCollection = 0;
	      float MinCollection = 0;
	      ResultSet rs;
	      rs = stmt.executeQuery(getMaxViews);
	      if (rs.next())
	      {
	    	   MaxViews = rs.getFloat("maxviews");
	    	   //System.out.println("MAXVIEWS");
	      }
	      //float MaxViews =  129177616;
	       rs = stmt.executeQuery(getMaxLikes_dislikes);
	      if (rs.next())
	      {
	    	  Maxdelta_likes =  rs.getFloat("deltalike");
	    	  //System.out.println("Maxdelta_likes");
	      }
	      //float Maxdelta_likes = 765200;
	      rs = stmt.executeQuery(getMaxCollection);
	      if (rs.next())
	      {
	    	  MaxCollection =  rs.getFloat("maxcoll");
	    	  //System.out.println("MaxCollection");
	    	  
	      }
	      rs = stmt.executeQuery(getMinCollection);
	      if (rs.next())
	      {
	    	  MinCollection =  rs.getFloat("mincoll");
	    	  //System.out.println("MinCollection");
	    	  
	      }
	      //float MaxCollection =  331062432;
	      //for each movie in database
	      for (int i =0;i<movielist.size();i++)
	      {
	    	  //get views
	    	  String getViews = "select YT_views from augur_test2 where movie_name = ?";
	    	  preparedStatement = conn.prepareStatement(getViews);
	    	  preparedStatement.setString(1,movielist.get(i));
	    	  rs = preparedStatement.executeQuery();
	    	  float views = 0;
	    	  if (rs.next())
	    		  {
	    		  views= rs.getFloat("YT_views");
	    		  //System.out.println(movielist.get(i)+"VIEWS"+views);
	    		  }
	    	  //get delta_likes
	    	  String getDeltaLike = "select like_dislike from augur_test2 where movie_name = ?";
	    	  preparedStatement = conn.prepareStatement(getDeltaLike);
	    	  preparedStatement.setString(1,movielist.get(i));
	    	  float delta_likes = 0;
	    	  rs = preparedStatement.executeQuery();
	    	  if (rs.next())
	    	  {
	    		  delta_likes = rs.getFloat("like_dislike");
	    		  //System.out.println(movielist.get(i)+"like_dislike"+delta_likes);
	    	  }
	    	  //calculate hype factor
	    	  float HF = (views/MaxViews)+(delta_likes/Maxdelta_likes);
	    	  String update = "UPDATE augur_test2 SET HF = ? WHERE movie_name = ?";
			  preparedStatement = conn.prepareStatement(update);
			  preparedStatement.setDouble(1, HF);
			  preparedStatement.setString(2, movielist.get(i));
			  preparedStatement.executeUpdate();
	    	  //get sentiment factor
	    	  String getSF =  "select var_coll from augur_test2 where movie_name = ?";
	    	  preparedStatement = conn.prepareStatement(getSF);
	    	  preparedStatement.setString(1,movielist.get(i));
	    	  String SF = "";
	    	  rs = preparedStatement.executeQuery();
	    	  char trigger = 'X';
	    	  if (rs.next())
	    	  {
	    		  SF = rs.getString("var_coll");
	    		  trigger = SF.charAt(0);
	    		  //System.out.println(movielist.get(i)+"var_coll "+ SF + "trigger "+trigger);
	    	  }
	    	  float SFmin=0,SFmax=0;
	    	//depending on SF set SFmax,SFmin
	    	  switch(trigger)
	    	  {
	    	  case 'A':
	    		  SFmin=300000000;
	    		  SFmax=MaxCollection;
	    		  break;
	    	  case 'B':
	    		  SFmin=270000000;
	    		  SFmax=300000000;
	    		  break;
	    	  case 'C':
	    		  SFmin=240000000;
	    		  SFmax=270000000;
	    		  break;
	    	  case 'D':
	    		  SFmin=210000000;
	    		  SFmax=240000000;
	    		  break;
	    	  case 'E':
	    		  SFmin=180000000;
	    		  SFmax=210000000;
	    		  break;
	    	  case 'F':
	    		  SFmin=150000000;
	    		  SFmax=180000000;
	    		  break;
	    	  case 'G':
	    		  SFmin=120000000;
	    		  SFmax=150000000;
	    		  break;
	    	  case 'H':
	    		  SFmin=90000000;
	    		  SFmax=120000000;
	    		  break;
	    	  case 'I':
	    		  SFmin=60000000;
	    		  SFmax=90000000;
	    		  break;
	    	  case 'J':
	    		  SFmin=30000000;
	    		  SFmax=60000000;
	    		  break;
	    	  case 'K':
	    		  SFmin=10000000;
	    		  SFmax=30000000;
	    		  break;
	    	  case 'L':
	    		  SFmin=MinCollection;
	    		  SFmax=10000000;
	    		  break;  
    		  default:
    			  //System.out.println("default :: "+trigger);
    			  SFmin=0;
    			  SFmax=0;
	    	  }
	    	  //calculate collection
	    	  float pred_BO = SFmin + (HF/2)*(SFmax-SFmin);
	    	  //update table
	    	  String update_predBO = "UPDATE augur_test2 SET pred_collection = ? WHERE movie_name = ?";
	    	  PreparedStatement ps = conn.prepareStatement(update_predBO);
	    	  ps.setDouble(1,pred_BO);
	    	  ps.setString(2, movielist.get(i));
	    	  ps.executeUpdate();
	    	  //System.out.println(movielist.get(i)+":::::"+pred_BO);
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
	   String movie_name = args[0].replace("_", " ");//for anirrudha's code.
	   update_predBoxOffice(movie_name);
   }//end main
}//end FirstExample
