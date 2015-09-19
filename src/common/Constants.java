package common;

import java.io.File;

public class Constants {

	public static final String FILE_SEPERATOR = File.separator;
	public static final String HACK_HOME = System.getenv("HACK_HOME");
	public static final String INPUT_FILE_DIR = HACK_HOME+FILE_SEPERATOR+"dataFolder";
	public static final String PROCESS_DIR = HACK_HOME+FILE_SEPERATOR+"proc";
	public static final String FILE1 = HACK_HOME+FILE_SEPERATOR+"file_1.csv";
	public static final String CSV_SEPERATOR = ",";
	public static final int READ_REC_BUFFER=1;
}
