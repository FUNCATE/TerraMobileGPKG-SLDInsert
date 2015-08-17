package br.funcate.terramobile;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class ImportSLDToGPKG {

	public static void main(String[] args) throws ClassNotFoundException, IOException {

		String filePath = "/dados/temp/terramobile-gpkg/inpe-boeing-small.gpkg";
		
		ImportSLDToGPKG.createStyleTable(filePath);
		ArrayList<Map<String, String>> layers = ImportSLDToGPKG.getLayers(filePath);
		for (Map<String, String> layer : layers) {
			String layerName = (String)layer.keySet().toArray()[0];
			String type = layer.get(layerName);
			String sld = ImportSLDToGPKG.getSLD(type.toLowerCase());
			if(!sld.isEmpty())
			{
				ImportSLDToGPKG.insertSLD(layerName, sld, filePath);	
			}
			
		}
	}
	
	private static void createStyleTable(String filePath) throws ClassNotFoundException
	{
		String sql = "create table if not exists TM_STYLE "
				+ "(LAYER_NAME text primary key not null,"
				+ "SLD_XML text);";
		
		// load the sqlite-JDBC driver using the current class loader
	    Class.forName("org.sqlite.JDBC");

	    Connection connection = null;
	    try
	    {
	      // create a database connection
	      connection = DriverManager.getConnection("jdbc:sqlite:" + filePath);
	      Statement statement = connection.createStatement();
	      statement.setQueryTimeout(30);  // set timeout to 30 sec.

	      statement.execute(sql);
	      	      
	      statement.close();
	      connection.close();
	    }
	    catch(SQLException e)
	    {
	      System.err.println(e.getMessage());
	    }
	    finally
	    {
	      try
	      {
	        if(connection != null)
	          connection.close();
	      }
	      catch(SQLException e)
	      {
	        // connection close failed.
	        System.err.println(e);
	      }
	    }
	}
	
	private static ArrayList<Map<String, String>> getLayers(String filePath) throws ClassNotFoundException
	{
		ArrayList<Map<String, String>> layers= new ArrayList<Map<String,String>>();
		String sql = "select gc.table_name, ggc.geometry_type_name "
				+ "from gpkg_contents gc, gpkg_geometry_columns ggc "
				+ "where gc.table_name = ggc.table_name and gc.data_type = 'features'";
		
		// load the sqlite-JDBC driver using the current class loader
	    Class.forName("org.sqlite.JDBC");

	    Connection connection = null;
	    try
	    {
	      // create a database connection
	      connection = DriverManager.getConnection("jdbc:sqlite:" + filePath);
	      Statement statement = connection.createStatement();
	      statement.setQueryTimeout(30);  // set timeout to 30 sec.

	      ResultSet rs = statement.executeQuery(sql);
	      
	      while(rs.next())
	      {
	    	  String tableName = rs.getString("table_name");
	    	  String geometry_type_name = rs.getString("geometry_type_name");
	    	  HashMap<String, String> layer = new HashMap<String, String>();
	    	  layer.put(tableName,geometry_type_name);
	    	  layers.add(layer);
	      }
	      
	      rs.close();
	      statement.close();
	      connection.close();
	    }
	    catch(SQLException e)
	    {
	      System.err.println(e.getMessage());
	    }
	    finally
	    {
	      try
	      {
	        if(connection != null)
	          connection.close();
	      }
	      catch(SQLException e)
	      {
	        // connection close failed.
	        System.err.println(e);
	      }
	    }
	    return layers;
	}
	private static void insertSLD(String layer, String sld, String filePath) throws ClassNotFoundException
	{
		String sql = "insert into tm_style values (?,?)";
		
		// load the sqlite-JDBC driver using the current class loader
	    Class.forName("org.sqlite.JDBC");

	    Connection connection = null;
	    try
	    {
	      // create a database connection
	      connection = DriverManager.getConnection("jdbc:sqlite:" + filePath);
	      PreparedStatement pstatement = connection.prepareStatement(sql);
	      pstatement.setQueryTimeout(30);  // set timeout to 30 sec.

	      pstatement.setString(1, layer);
	      pstatement.setString(2, sld);
	      pstatement.execute();

	      pstatement.close();
	      connection.close();
	    }
	    catch(SQLException e)
	    {
	      System.err.println(e.getMessage());
	    }
	    finally
	    {
	      try
	      {
	        if(connection != null)
	          connection.close();
	      }
	      catch(SQLException e)
	      {
	        // connection close failed.
	        System.err.println(e);
	      }
	    }
	}
	
	private static String getSLD(String type) throws IOException
	{
		if(type.contains("point"))
		{
			return ImportSLDToGPKG.getPointSLD();
		} else if(type.contains("line"))
		{
			return ImportSLDToGPKG.getLineSLD();
		}  if(type.contains("polygon"))
		{
			return ImportSLDToGPKG.getPolygonSLD();
		} 
		return "";
		
	}
	
	private static String getPolygonSLD() throws IOException
	{
		return ImportSLDToGPKG.readFile(new File("polygon-sld.xml"));
	}
	private static String getPointSLD() throws IOException
	{
		return ImportSLDToGPKG.readFile(new File("point-sld-inline.xml"));
	}
	private static String getLineSLD() throws IOException
	{
		return ImportSLDToGPKG.readFile(new File("line-sld.xml"));
	}


	  public static String readFile(File file) throws IOException {
	      int len;
	      char[] chr = new char[4096];
	      final StringBuffer buffer = new StringBuffer();
	      final FileReader reader = new FileReader(file);
	      try {
	          while ((len = reader.read(chr)) > 0) {
	              buffer.append(chr, 0, len);
	          }
	      } finally {
	          reader.close();
	      }
	      return buffer.toString();
	  }

}
