package myio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import common.AppContext;
import dataobj.DataRecord;
import dataobj.SubscrRule;
import myio.*;
import static common.Constants.CSV_SEPERATOR;
import static common.Constants.FILE1;
import static common.Constants.READ_REC_BUFFER;
import static common.Constants.SUBSCR_TBL;

public class FileHandler 
{	
	private static void procForSubscr(AppContext ctx
										, HashMap<String,HashMap<String,ArrayList<String>>> attrMap
										, DataRecord dr)
	{
		ArrayList<String> itemList = new ArrayList<String>();
		
		HashMap<String,ArrayList<String>> subAttrMap = new HashMap<String,ArrayList<String>>();
		
		if(attrMap.containsKey(dr.getattribute_name()))
		{
			subAttrMap = attrMap.get(dr.getattribute_name());
			if(subAttrMap.containsKey(dr.getattribute_value()))
			{
				itemList = subAttrMap.get(dr.getattribute_value());
			}
		}
		else
		{
			subAttrMap = new HashMap<String,ArrayList<String>>();
			itemList = new ArrayList<String>();
		}
		
		itemList.add(dr.getitem());
		subAttrMap.put(dr.getattribute_value(), itemList);
		attrMap.put(dr.getattribute_name(), subAttrMap);	
	}
	
	private static ArrayList<String> procGTE(AppContext ctx, HashMap<String,ArrayList<String>> subAttrMap, String value)
	{
		ArrayList<String> idList = new ArrayList<String>();
		
		for(String key:subAttrMap.keySet())
		{
			if(key.compareTo(value) >= 0)
			{
				idList.addAll(subAttrMap.get(key));
			}
		}
		
		return idList;
	}
	
	private static ArrayList<String> procLTE(AppContext ctx, HashMap<String,ArrayList<String>> subAttrMap, String value)
	{
		ArrayList<String> idList = new ArrayList<String>();
		
		for(String key:subAttrMap.keySet())
		{
			if(key.compareTo(value) <= 0)
			{
				idList.addAll(subAttrMap.get(key));
			}
		}
		
		return idList;
	}
	
	private static ArrayList<String> procE(AppContext ctx, HashMap<String,ArrayList<String>> subAttrMap, String value)
	{
		ArrayList<String> idList = new ArrayList<String>();
		
		for(String key:subAttrMap.keySet())
		{
			if(key.equals(value))
			{
				idList.addAll(subAttrMap.get(key));
			}
		}
		
		return idList;
	}
	
	private static ArrayList<String> procAND(ArrayList<String> idList, ArrayList<String> locIdList)
	{
		ArrayList<String> andIdList = new ArrayList<String>();
		
		for(String key:locIdList)
		{
			if(idList.contains(key))
			{
				andIdList.add(key);
			}
		}
		
		return andIdList;
	}
	
	private static ArrayList<String> removeDupRec(ArrayList<String> idList)
	{
		HashSet<String> hs = new HashSet<String>();
		
		hs.addAll(idList);
		idList.clear();
		idList.addAll(hs);
		
		return idList;
	}
	
	private static ArrayList<String> processCondition(AppContext ctx, HashMap<String,ArrayList<String>> subAttrMap, SubscrRule sr)
	{
		ArrayList<String>	locIdList	= new ArrayList<String>();
		String				key			= sr.getc_src() + "|" + sr.getc_cond() + "|" + sr.getc_value(); 
		
		if(subAttrMap == null)
		{
			return locIdList;
		}
		
		if(ctx.glbSRMap.containsKey(key))
		{
			return ctx.glbSRMap.get(key);
		}
		
		if(sr.getc_cond().equals(">="))
		{
			locIdList = procGTE(ctx, subAttrMap, sr.getc_value());
		}
		else if(sr.getc_cond().equals("<="))
		{
			locIdList = procLTE(ctx, subAttrMap, sr.getc_value());
		}
		else if(sr.getc_cond().equals("="))
		{
			locIdList = procE(ctx, subAttrMap, sr.getc_value());
		}
		
		ctx.glbSRMap.put(key, locIdList);
	
		return locIdList;
	}
	
	private static ArrayList<String> processOperator(String prevOperator
													, ArrayList<String> idList
													, ArrayList<String> locIdList)
	{
		if(prevOperator.equals("") || prevOperator.equals("OR"))
		{
			idList.addAll(locIdList);
			idList = removeDupRec(idList);
		}
		else if(prevOperator.equals("AND"))
		{
			idList = procAND(idList, locIdList);
		}
		
		return idList;
	}
	
	private static void printAttrMap(HashMap<String,HashMap<String,ArrayList<String>>> attrMap)
	{
		System.out.println("\nPrinting attribute map for reference" 
							+"\n================================");
		Set<String> keys = attrMap.keySet();
		for(String key0: keys){
			System.out.println("\n" + key0);
			HashMap<String,ArrayList<String>> subMap = attrMap.get(key0); 
			Set<String> keys1 = subMap.keySet();
			
			for(String key1:keys1){
				System.out.println(key1 +  "|" + subMap.get(key1).toString());
			}
		}
	}
	
	private static void processNotification(AppContext ctx
											, HashMap<String,HashMap<String,String>> itemMap
											, ArrayList<String> idList
											, String subscrId
											, ArrayList<String> attrList
											, String insertTimeStr) throws Exception
	{
		int		count		= 0;
		String insertStr	= "INSERT INTO " + SUBSCR_TBL + " ('subscr_id', 'item_id', 'attribute_name', 'attribute_value', 'lchg_time') VALUES (?,?,?,?,?);";
		
		ctx.conn.setAutoCommit(false);
		PreparedStatement prep = ctx.conn.prepareStatement(insertStr);
		
		if(idList.size() > 0)
		{
			System.out.println(	  "\nNotification for subscriber : " 
								+ subscrId 
								+ "\n============================\n");
			
			for(String key0:idList)
			{
				System.out.println("Item [" + key0 + "] has been modified.");
				for(String key1:attrList)
				{
					count = count + 1;
					System.out.println("\t" + insertTimeStr + "|" + key0 + "|" + key1 + "|"
											+ itemMap.get(key0).get(key1));
					prep.setString(1, subscrId);
					prep.setString(2, key0);
					prep.setString(3, key1);
					prep.setString(4, itemMap.get(key0).get(key1));
					prep.setString(5, insertTimeStr);
					
					prep.addBatch();
				}
			}
			
			int[] updateCount = prep.executeBatch();
			ctx.conn.commit();
			
			System.out.println("\n" + count + " Records inserted Successfully.\n");	
		}
		else
		{
			System.out.println(	  "\nNo Notification for subscriber : " 
					+ subscrId + "\n");
		}
	}
	
	public static void processSubscr(AppContext ctx 
					, HashMap<String,HashMap<String,ArrayList<String>>> attrMap
					, HashMap<String,HashMap<String,String>> itemMap
					, String insertTimeStr) throws Exception 
	{
		HashMap<String, ArrayList<SubscrRule>> ruleMap = mySqlite.getSubscrRule(ctx);
		HashMap<String,ArrayList<String>> subAttrMap = new HashMap<String,ArrayList<String>>();
		
		ArrayList<String> idList = new ArrayList<String>();
		ArrayList<String> locIdList = new ArrayList<String>();
		ArrayList<String> attrList = new ArrayList<String>();
		
		String prevOperator = "";
		String subscrId = "";
		
		for(String key:ruleMap.keySet())
		{
			idList.clear();
			prevOperator = "";
			attrList.clear();
			for(SubscrRule sr:ruleMap.get(key))
			{
				locIdList.clear();
				attrList.add(sr.getc_src());
				subAttrMap = attrMap.get(sr.getc_src());
				
				locIdList = processCondition(ctx, subAttrMap, sr);
				
				idList = processOperator(prevOperator, idList, locIdList);
				
				prevOperator = sr.getc_op();
				subscrId = sr.getsubscr_id();
			}
			
			processNotification(ctx, itemMap, idList, subscrId, attrList, insertTimeStr);
		}
		
		printAttrMap(attrMap);

	}
	
	private static void procForItemMap(AppContext ctx, HashMap<String,HashMap<String,String>> itemMap, DataRecord dr)
	{
		HashMap<String,String> subMap = new HashMap<String,String>();
		
		if(itemMap.containsKey(dr.getitem()))
		{
			subMap = itemMap.get(dr.getitem());
		}
		else
		{
			subMap = new HashMap<String,String>();
		}
		
		subMap.put(dr.getattribute_name(), dr.getattribute_value());
		itemMap.put(dr.getitem(), subMap);
	}
	
	public static void processInputFile(AppContext ctx, String fileName, String insertTimeStr) throws SQLException 
	{		
		HashMap<String, DataRecord> dataMap = new HashMap<String, DataRecord>();
		
		HashMap<String,HashMap<String,ArrayList<String>>> attrMap = new HashMap<String,HashMap<String,ArrayList<String>>>();
		
		HashMap<String,HashMap<String,String>> itemMap = new HashMap<String,HashMap<String,String>>();
		
		int	batchNum = 1;
		
		BufferedReader br = null;
		String line = "";
		String key = "";

		try 
		{
			br = new BufferedReader(new FileReader(fileName));
			
			while ((line = br.readLine()) != null) 
			{	
				DataRecord dr = new DataRecord(line.split(CSV_SEPERATOR), insertTimeStr, batchNum);
	
				procForSubscr(ctx, attrMap, dr);
				procForItemMap(ctx, itemMap, dr);
				key = dr.getitem() + "|" + dr.getattribute_name();
				dataMap.put(key, dr);
				
				if(dataMap.size() > READ_REC_BUFFER) {
					batchNum = batchNum + 1;
					processSubscr(ctx, attrMap, itemMap, insertTimeStr);
					mySqlite.updateData(ctx, dataMap);
					dataMap.clear();
					attrMap.clear();
				}
			}

			processSubscr(ctx, attrMap, itemMap, insertTimeStr);
			mySqlite.updateData(ctx, dataMap);
			dataMap.clear();
			attrMap.clear();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
		}
	}
	
		System.out.println("Done");
	}
}
