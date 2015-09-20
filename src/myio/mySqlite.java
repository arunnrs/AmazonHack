package myio;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import common.*;
import dataobj.DataRecord;
import dataobj.SubscrRule;
import static common.Constants.MAIN_TBL;
import static common.Constants.ITEM_ATTR_TBL;
import static common.Constants.ATTR_TBL;
import static common.Constants.SUBSCR_TBL;
import static common.Constants.SUBSCR_QRY_TBL;

public class mySqlite {
	
	public static void getConnection(AppContext ctx)
	{
	    try {
	    	
		      Class.forName("org.sqlite.JDBC");
		      ctx.conn = DriverManager.getConnection("jdbc:sqlite:test.db");
		      System.out.println("Opened database successfully. \n");
		      ctx.stmt = ctx.conn.createStatement();		      
	    } catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		      System.exit(0);
	    }
	}
	

	public static void createTable(AppContext ctx, String stmtStr)
	{
	    try {
		      ctx.stmt.executeUpdate(stmtStr);
		      System.out.println("Table created successfully. \n");
	    } catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
	}

	public static void alterMainTbl(AppContext ctx, String newColName)
	{
	    try {
	    	String stmtStr = "ALTER TABLE " + MAIN_TBL + " ADD COLUMN '" + newColName + "' TEXT;";
		    ctx.stmt.executeUpdate(stmtStr);
		    System.out.println("New attribute added successfully :" + newColName + ". \n");
	    } catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
	}

	public static void deleteData(AppContext ctx, String stmtStr)
	{
	    try {
	    		ctx.conn.setAutoCommit(false);
	    		ctx.stmt.executeUpdate(stmtStr);
	    		System.out.println("Records deleted successfully. \n");
	    		ctx.conn.commit();
	    } catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
	}
	
	public static void insertData(AppContext ctx, String stmtStr)
	{
	    try {
	    		ctx.conn.setAutoCommit(false);
	    		ctx.stmt.executeUpdate(stmtStr);
	    		System.out.println("Records created successfully. \n");
	    		ctx.conn.commit();
	    } catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
	}

	public static HashMap<String, ArrayList<SubscrRule>> getSubscrRule(AppContext ctx) throws SQLException
	{
		ResultSet	rs		= null;
		HashMap<String, ArrayList<SubscrRule>> ruleMap = new HashMap<String, ArrayList<SubscrRule>>();
		String key = null;
		ArrayList<SubscrRule> srList = null;
		SubscrRule sr = null;
		
		try {
			rs = ctx.stmt.executeQuery( "SELECT * FROM " + SUBSCR_QRY_TBL + " ORDER BY subscr_id, id;" );
	      
		} catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
		
		while (rs.next()) {
			sr = new SubscrRule(rs);
			key = sr.getid() + "|" + sr.getsubscr_id();
			
			if(ruleMap.containsKey(key))
			{
				srList = ruleMap.get(key);
			}
			else
			{
				srList = new ArrayList<SubscrRule>();
			}
			
			srList.add(sr);
			ruleMap.put(key, srList);
		}
		
		return ruleMap;
	}

	
	
	public static Boolean getSqlRecs(AppContext ctx, String whereStr)
	{
		Boolean		ret		= false; 
		try {
			ResultSet rs = ctx.stmt.executeQuery( "SELECT * FROM " + MAIN_TBL + " WHERE " + whereStr + ";" );
			while ( rs.next()) {
				ret = true;
				Set<String> keys = ctx.attrMap.keySet();
				
				System.out.println("Item Key : " + rs.getString("id"));
				for(String key1: keys)
				{
					if(key1.equals("id")) continue;
					
					System.out.println(key1 + " : [" + rs.getString(key1) + "]");
				}
	      }
	      
			if(ret == true)
			{
				System.out.println("Fetch successfull. \n");
			}
			else
			{
				System.out.println("Record not found. \n");
			}
			
	      rs.close();
	      
		} catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
		
		return ret;
	}

	
	public static Boolean selectNotifications(AppContext ctx, String key, Boolean isLatest)
	{
		Boolean		ret		= false; 
		try {
		
			System.out.println("\nSubscriber Id : " + key);
			System.out.println("===============");
			
			ResultSet rs = null;
			if(isLatest == false)
			{
				rs = ctx.stmt.executeQuery( "SELECT * FROM " + SUBSCR_TBL + " WHERE subscr_id = '" + key + "';" );
			}
			else
			{
				rs = ctx.stmt.executeQuery( "SELECT * FROM " + SUBSCR_TBL + " WHERE subscr_id = '" + key 
											+ "' AND lchg_time in (SELECT max(lchg_time) from " + SUBSCR_TBL + " WHERE subscr_id = '" + key + "' GROUP BY subscr_id, item_id, attribute_name);");
			}
			
			while ( rs.next()) {
				ret = true;
				System.out.println(rs.getString("lchg_time") + "|" + rs.getString("item_id") + " = " + rs.getString("attribute_name") + " | " + rs.getString("attribute_value"));
			}
	      
			if(ret == true)
			{
				System.out.println("Fetch successfull. \n");
			}
			else
			{
				System.out.println("Record not found. \n");
			}
			
	      rs.close();
	      
		} catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
		
		return ret;
	}

	
	public static Boolean selectItem(AppContext ctx, String key)
	{
		Boolean		ret		= false; 
		try {
		
			System.out.println("\nItem Key : " + key);
			System.out.println("==========");
			
			ResultSet rs = ctx.stmt.executeQuery( "SELECT * FROM " + ITEM_ATTR_TBL + " WHERE ID = '" + key + "';" );
			while ( rs.next()) {
				ret = true;
				System.out.println(rs.getString("lchg_time") + "|" + rs.getString("attribute_name") + " = " + rs.getString("attribute_value"));
			}
	      
			if(ret == true)
			{
				System.out.println("Fetch successfull. \n");
			}
			else
			{
				System.out.println("Record not found. \n");
			}
			
	      rs.close();
	      
		} catch ( Exception e ) {
		      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
		
		return ret;
	}
	
	public static void loadAttrMap(AppContext ctx){
		try {
			
		      ResultSet rs = ctx.stmt.executeQuery( "SELECT * FROM " + ATTR_TBL + ";");
		      while ( rs.next()) {
		    	  ctx.attrMap.put(rs.getString("ATTRIBUTE_NAME"), rs.getString("ATTRIBUTE_TYPE"));
		    	  System.out.println(rs.getString("ATTRIBUTE_NAME"));
		      }
		      rs.close();
		      System.out.println("Attribute list loaded successfully. \n");
			} catch ( Exception e ) {
			      System.err.println( e.getClass().getName() + ": " + e.getMessage() );
		    }		
	}
	
	public static void checkAndAddCol(AppContext ctx, String key){
		if(!ctx.attrMap.containsKey(key))
		{
			alterMainTbl(ctx, key);
			ctx.attrMap.put(key, "TEXT");
			insertData(ctx, "INSERT INTO ATTR_TBL (ATTRIBUTE_NAME, ATTRIBUTE_TYPE) VALUES ('" + key + "', 'TEXT');");
		}
	}
	
	public static String prepareInsertStr(AppContext ctx){
		String	insertStr	= "INSERT INTO " + MAIN_TBL;
		String valueStr = "";
		String nameStr	= "";
		Set<String> keys = ctx.attrMap.keySet();
		for(String key: keys)
		{
			if (!valueStr.equals(""))
			{
				nameStr = nameStr + ", '" + key + "'"; 
				valueStr = valueStr + ",?";
			}
			else
			{
				nameStr = "'" + key + "'";
				valueStr = "?";
			}
		}
		
		insertStr = insertStr + " (" + nameStr + ") VALUES (" + valueStr + ");";
		
		System.out.println("Insert String : " + insertStr);
		
		return insertStr;
	}
	
	private static Boolean isSkip(AppContext ctx, DataRecord dr) throws SQLException
	{
		Boolean	ret		= false;

		ResultSet rs = ctx.stmt.executeQuery( "SELECT * FROM " + ITEM_ATTR_TBL + " WHERE id = '" + dr.getitem() + "'"
										+ " AND attribute_name = '" + dr.getattribute_name() + "'" 
										+ " AND "
										+ " ("
										+ " (lchg_time > '" + dr.getinsertTimeStr() + "')" 
											+ " OR "
										+ " ((lchg_time = '" + dr.getinsertTimeStr() + "') AND (batch_num > '" + dr.getbatch_num() + "')))"
										+";");
		while (rs.next()) {
			ret = true;
		}
		
		return ret;
	}
	
	public static void updateData(AppContext ctx, HashMap<String, DataRecord> dataMap)
	{
		String insertStr	= "INSERT OR REPLACE INTO " + ITEM_ATTR_TBL + " ('id', 'attribute_name', 'attribute_value', 'lchg_time', 'batch_num') VALUES (?,?,?,?,?);";
		
		int	skipCount = 0;
		
		DataRecord dr = new DataRecord(); 
		try{
			ctx.conn.setAutoCommit(false);
			PreparedStatement prep = ctx.conn.prepareStatement(insertStr);
			
			System.out.println("\nGoing to update data");
			Set<String> keys = dataMap.keySet();
			for(String key: keys)
			{
				
				
				dr = dataMap.get(key);
				if(isSkip(ctx, dr))
				{
					skipCount = skipCount + 1;
					System.out.println("Skipped:" + dr.getitem() + "|" + dr.getattribute_name() + "|" + dr.getattribute_value() + "|" + dr.getinsertTimeStr() + "|" + dr.getbatch_num());
					continue;
				}
				prep.setString(1, dr.getitem());
				prep.setString(2, dr.getattribute_name());
				prep.setString(3, dr.getattribute_value());
				prep.setString(4, dr.getinsertTimeStr());
				prep.setInt(5, dr.getbatch_num());
				prep.addBatch();
				
				System.out.println(dr.getitem() + "|" + dr.getattribute_name() + "|" + dr.getattribute_value() + "|" + dr.getinsertTimeStr() + "|" + dr.getbatch_num());
			}
			
			int[] updateCount = prep.executeBatch();
			ctx.conn.commit();
			
			System.out.println((dataMap.size() - skipCount) + " Records inserted/updated Successfully.\n" + skipCount + " Records skipped.\n");
			
		} catch ( Exception e ) {
		    System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	    }
	}
	
}
