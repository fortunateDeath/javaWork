package org.webapp.jettyweb;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSetMetaData;

public class DataAccess 
{
    private static String dbURL = "jdbc:derby:/home/nathan/data/derby/test/testdb;";
    private static String tableName = "Persons";
    // jdbc Connection
    private static Connection conn = null;
    private static Statement stmt = null;
    
    public static String newPerson(int id, String lastName, String firstName, String address,String cityName)
    {
    	try{
    		createConnection();
    		insertPerson(id,lastName,firstName,address,cityName);
    		String allPeeps = selectPeople();
    		shutdown();
    		return allPeeps;
    	}
    	catch(Exception e)
    	{
    		return "Could not connect to the database";
    	}
    }

    private static void createConnection() throws Exception
    {
        try
        {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance();
            //Get a connection
            conn = DriverManager.getConnection(dbURL); 
            Statement s = conn.createStatement();
            s.executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
                "'derby.database.fullAccessUsers', null)");
        }
        catch (Exception except)
        {
        	throw except;
        }
    }
    
    private static void insertPerson(int id, String lastName, String firstName, String address,String cityName)
    {
        try
        {
            stmt = conn.createStatement();
            stmt.execute("insert into " + tableName + " values (" +
                    id + ",'" + lastName + "','" + firstName + "','"
            		+ address + "','" + cityName +"')");
            stmt.close();
        }
        catch (SQLException sqlExcept)
        {
            sqlExcept.printStackTrace();
        }
    }
    
    public static String selectPeople()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("");
        try
        {
            stmt = conn.createStatement();
            ResultSet results = stmt.executeQuery("select * from " + tableName);
            ResultSetMetaData rsmd = results.getMetaData();
            int numberCols = rsmd.getColumnCount();
            for (int i=1; i<=numberCols; i++)
            {
                //print Column Names
                sb.append(rsmd.getColumnLabel(i)+"\t\t");  
            }

            sb.append("\n-------------------------------------------------");

            while(results.next())
            {
                int id = results.getInt(1);
                String lastName = results.getString(2);
                String firstName = results.getString(3);
                String address = results.getString(4);
                String cityName = results.getString(5);
                System.out.println(id + "\t\t" + lastName + "\t\t" + cityName);
                sb.append(id + "\t\t" + lastName + "\t\t" + firstName + "\t\t" + address + "\t\t" + cityName + "\n");
            }
            results.close();
            stmt.close();
        }
        catch (SQLException sqlExcept)
        {
            sqlExcept.printStackTrace();
        }
        return sb.toString();
    }
    
    private static void shutdown()
    {
        try
        {
            if (stmt != null)
            {
                stmt.close();
            }
            if (conn != null)
            {
                DriverManager.getConnection(dbURL + ";shutdown=true");
                conn.close();
            }           
        }
        catch (SQLException sqlExcept)
        {
            
        }

    }

}
