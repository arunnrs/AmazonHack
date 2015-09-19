package service;

import locio.FileHandler;
import java.io.File;
import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;

import static common.Constants.PROCESS_DIR;
import static common.Constants.FILE_SEPERATOR;
import static common.Constants.INPUT_FILE_DIR;

public class WhackAHack {
	public static void moveFileToProcDir(File mvFile)
	{
		//System.out.println("Before Format : " + mvFile.lastModified());
		  	
		SimpleDateFormat insertTime = new SimpleDateFormat("ddMMMyyyy_HHmmss_");
		String insertTimeS=insertTime.format(mvFile.lastModified());
		
		Calendar cal = Calendar.getInstance();
		SimpleDateFormat currentTime = new SimpleDateFormat("HHmmss_");
		String currentTimeS=currentTime.format(cal.getTime());
        System.out.println( currentTime.format(cal.getTime()) );
		System.out.println("insertTimeS "+insertTimeS);
		System.out.println("currentTimeS "+currentTimeS);
		String newFile=PROCESS_DIR+FILE_SEPERATOR+currentTimeS+insertTimeS+"_"+mvFile.getName();
		mvFile.renameTo(new File(newFile));
		
	}
	
	public static void main(String[] args) throws InterruptedException 
	  {
		 // FileHandler.processInputFile();
		//for(int loop=0;loop<2;loop++)
		//{
			File processDir = new File(PROCESS_DIR);
			if (!processDir.exists())
			{
				processDir.mkdir();
			}
			//moveFileToProcDir(new File("D:\\HACK\\dataFolder\\file_1.csv"));	
			File[] files = new File(INPUT_FILE_DIR).listFiles();
			 
			List<String> results = new ArrayList<String>();
			for (File file : files) {
			    if (file.isFile()) {
			        results.add(file.getName());
			    	//moveFileToProcDir(new File(PROCESS_DIR+FILE_SEPERATOR+file.getName()));
			        moveFileToProcDir(file);
			    }
			}
			System.out.println(results); 
			Thread.sleep(10000);
		//}
	  }	  
}
