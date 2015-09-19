package locio;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import dataobj.DataRecord;
import static common.Constants.CSV_SEPERATOR;
import static common.Constants.FILE1;
import static common.Constants.READ_REC_BUFFER;

public class FileHandler 
{
	

	public static void processInputFile() 
	{
		HashMap<String,HashMap<String,String>> dataMap = new HashMap<String,HashMap<String,String>>(); 
		HashSet<String> hs=new HashSet<String>();
		HashMap<String,String> subMap = new HashMap<String,String>();
		
		BufferedReader br = null;
		String line = "";
		//DataRecord dr=null;
		
		try 
		{
			br = new BufferedReader(new FileReader(FILE1));
			while ((line = br.readLine()) != null) 
			{
				DataRecord dr = new DataRecord(line.split(CSV_SEPERATOR));
				
				if(dataMap.containsKey(dr.getitem()))
				{
					subMap = dataMap.get(dr.getitem());
				}else
				{
					if(hs.size() > READ_REC_BUFFER)
					{
						Set<String> keys = dataMap.keySet();
						for(String key: keys)
						{
							System.out.println("keyItem "+key);
							HashMap<String,String> sMap = dataMap.get(key);
							Set<String> skeys = sMap.keySet();
							for(String skey : skeys)
							{
								System.out.println("key "+skey+" data "+sMap.get(skey));
						
							}
						}
						hs.clear();
					}
					hs.add(dr.getitem());
					subMap = new HashMap<String,String>();
				}
				subMap.put(dr.getattribute_name(), dr.getattribute_value());
				dataMap.put(dr.getitem(),subMap);
				
				/*System.out.println("DATARecord [item= " + dr.getitem() 
	                                 + " , attribute_name=" + dr.getattribute_name()
	                                 + " , attribute_value=" + dr.getattribute_value()
	                                 + "]");
	                                 */
			}
			System.out.println("update rest of the map in DB ");
		} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
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
