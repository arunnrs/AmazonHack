package common;

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;

public class AppContext {
	public Connection				conn	= null;
	public Statement				stmt	= null;
	public HashMap<String, String>	attrMap	= new HashMap<String, String>();
	public HashMap<String, ArrayList<String>>	glbSRMap	= new HashMap<String, ArrayList<String>>();
}
