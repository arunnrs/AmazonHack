package hackmain;

import java.io.File;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import common.*;
import myio.*;
import static common.Constants.PROCESS_DIR;
import static common.Constants.INPUT_FILE_DIR;
import static common.Constants.FILE_SEPERATOR;
import static common.Constants.MAIN_TBL;
import static common.Constants.ITEM_ATTR_TBL;
import static common.Constants.ATTR_TBL;
import static common.Constants.SUBSCR_TBL;
import static common.Constants.SUBSCR_QRY_TBL;

public class WhackAHack {

	private static void createTables(AppContext ctx) {
		String stmtStr = "";
		
		/*
		stmtStr = "CREATE TABLE " + MAIN_TBL 
						+ " (id TEXT PRIMARY KEY NOT NULL"
						+ ");";
		mySqlite.createTable(ctx, stmtStr);
		
		stmtStr = "CREATE TABLE " + ATTR_TBL 
				+ " (attribute_name TEXT PRIMARY KEY     NOT NULL,"
				+ " attribute_type         TEXT    NOT NULL " 
				+ ");";
		//mySqlite.createTable(ctx, stmtStr);
		
		//mySqlite.insertData(ctx, "INSERT INTO " + ATTR_TBL + " VALUES('id', 'TEXT');");
		*/
		
		stmtStr = "CREATE TABLE " + ITEM_ATTR_TBL 
				+ " (id TEXT NOT NULL "
				+ ", attribute_name TEXT NOT NULL"
				+ ", attribute_value TEXT "
				+ ", lchg_time DATE NOT NULL"
				+ ", batch_num INT NOT NULL"
				+ ", PRIMARY KEY (id, attribute_name)"
				+ ");";
		mySqlite.createTable(ctx, stmtStr);
	
		stmtStr = "CREATE TABLE " + SUBSCR_QRY_TBL
				+ " (id INT NOT NULL"
				+ ", subscr_id TEXT NOT NULL"
				+ ", c_src TEXT NOT NULL"
				+ ", c_cond TEXT"
				+ ", c_value TEXT"
				+ ", c_op TEXT"
				+ ");";
		mySqlite.createTable(ctx, stmtStr);
		
		//insertSubscr(ctx);

		stmtStr = "CREATE TABLE " + SUBSCR_TBL
				+ " (subscr_id TEXT NOT NULL"
				+ ", item_id TEXT NOT NULL"
				+ ", lchg_time DATE NOT NULL"
				+ ", attribute_name TEXT"
				+ ", attribute_value TEXT"
				+ ");";
		mySqlite.createTable(ctx, stmtStr);
	}

	private static void insertSubscr(AppContext ctx)
	{
		mySqlite.insertData(ctx, "INSERT INTO " + SUBSCR_QRY_TBL 
				+ " ('id', 'subscr_id', 'c_src', 'c_cond', 'c_value', 'c_op') "
				+ " VALUES('1', 'Moh', 'release date', '>=', '01-01-2000', '');");
		mySqlite.insertData(ctx, "INSERT INTO " + SUBSCR_QRY_TBL 
				+ " ('id', 'subscr_id', 'c_src', 'c_cond', 'c_value', 'c_op') "
				+ " VALUES('2', 'Arunn', 'publisher', '=', 'Addison-Wesley', 'AND');");
		mySqlite.insertData(ctx, "INSERT INTO " + SUBSCR_QRY_TBL 
				+ " ('id', 'subscr_id', 'c_src', 'c_cond', 'c_value', 'c_op') "
				+ " VALUES('2', 'Arunn', 'list price', '>=', '10', '');");
		
	}

	private static void deleteRec(AppContext ctx)
	{
		//mySqlite.deleteData(ctx, "DELETE from " + ATTR_TBL + ";");
		//mySqlite.deleteData(ctx, "DELETE from " + ITEM_ATTR_TBL + ";");
		//mySqlite.deleteData(ctx, "DELETE from " + SUBSCR_TBL + ";");
		//mySqlite.deleteData(ctx, "DROP TABLE " + ITEM_ATTR_TBL + ";");
		//mySqlite.deleteData(ctx, "DROP TABLE " + SUBSCR_TBL + ";");
		//mySqlite.deleteData(ctx, "DELETE from " + SUBSCR_QRY_TBL + ";");
	}
	
	private static void init(AppContext ctx){
		mySqlite.getConnection(ctx);
		//deleteRec(ctx);
		//createTables(ctx);
	}
	
	public static void getRecs(AppContext ctx) {
		String whereStr = " `list price` = '7 USD' ";
		mySqlite.getSqlRecs(ctx, whereStr);
	}
	
	private static String getTimeStamp(File inpFile, String formatStr)
	{
		SimpleDateFormat insertTime = new SimpleDateFormat(formatStr);
		
		return (insertTime.format(inpFile.lastModified()));
	}
	
	public static void moveFileAndProcess(File mvFile, String currentTimeS) throws Exception
	{	  	
		AppContext ctx = new AppContext();
		init(ctx);
		
		System.out.println("\nGoing to process file : " + mvFile.getName() + "\n");
		
		String newFile = PROCESS_DIR+FILE_SEPERATOR + currentTimeS 
						+ getTimeStamp(mvFile, "ddMMMyyyy_HHmmss") 
						+ "_" + mvFile.getName();
		
		String insertTimeStr = getTimeStamp(mvFile, "dd-MMM-yyyy HH:mm:ss");
		
		mvFile.renameTo(new File(newFile));
		
		FileHandler.processInputFile(ctx, newFile, insertTimeStr);
	}
	
	private static void processFiles()
	{
		try {
			File processDir = new File(PROCESS_DIR);
			if (!processDir.exists())
			{
				processDir.mkdir();
			}
			for(int loop = 0; loop < 2; loop++)
			{
				if(loop > 0)
				{
					System.out.println("\n########## Going to sleep now ##########\n");
					Thread.sleep(10000);
				}
				
				File[] files = new File(INPUT_FILE_DIR).listFiles();
				
				if(files == null || files.length == 0)
				{
					System.out.println("No files found for processing.");
					continue;
				}
				
				Calendar cal = Calendar.getInstance();
				SimpleDateFormat currentTime = new SimpleDateFormat("HHmmss_");
				String currentTimeS=currentTime.format(cal.getTime());
				
				List<String> results = new ArrayList<String>();
				for (File file : files) 
				{
				    if (file.isFile()) 
				    {
				        results.add(file.getName());
				        moveFileAndProcess(file,currentTimeS);
				    }
				}
				System.out.println(results); 
			}
			
			System.out.println("\n########## Process completed. ##########\n");
	
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println( e.getClass().getName() + ": " + e.getMessage() );
			e.printStackTrace();
		}
	}
	
	public static void main(String args[])
	{
		AppContext ctx = new AppContext();
		init(ctx);
	
		// Fetch operations
		//mySqlite.selectItem(ctx, "13579");	// Fetch Single Record
		//mySqlite.selectNotifications(ctx, "Arunn", false); // Select notifications sent to a subscriber
		//mySqlite.selectNotifications(ctx, "Arunn", true); // Select only latest notifications sent to a subscriber
		
		// Process multiple files from input directory
		processFiles();
	}
}

