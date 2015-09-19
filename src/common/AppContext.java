package common;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;

public class AppContext {
	public Connection		conn	= null;
	public Statement		stmt	= null;
	public static HashMap<String,HashMap<String,String>> dataMap;
	
}
