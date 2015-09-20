package dataobj;

import java.sql.ResultSet;
import java.sql.SQLException;

public class SubscrRule {

	private int id;
	private String subscr_id;
	private String c_src;
	private String c_cond;
	private String c_value;
	private String c_op;
	
	public SubscrRule(ResultSet rs) throws SQLException
	{
		this.id = rs.getInt("id");
		this.subscr_id = rs.getString("subscr_id");
		this.c_src = rs.getString("c_src");
		this.c_cond = rs.getString("c_cond");
		this.c_value = rs.getString("c_value");
		this.c_op = rs.getString("c_op");
	}
	
	public int getid()
	{
		return this.id;
	}
	
	public String getsubscr_id()
	{
		return this.subscr_id;
	}
	
	public String getc_src()
	{
		return this.c_src;
	}
	
	public String getc_cond()
	{
		return this.c_cond;
	}
	
	public String getc_value()
	{
		return this.c_value;
	}
	
	public String getc_op()
	{
		return this.c_op;
	}
}
