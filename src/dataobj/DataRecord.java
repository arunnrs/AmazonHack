package dataobj;

public class DataRecord {

	private String item;
	private String attribute_name;
	private String attribute_value;
	private String lchg_time;
	private int batch_num;
	
	public DataRecord()
	{	
		
	}
	public DataRecord(String[] record, String insertTimeStr, int batch_num)
	{
		this.item=record[0];
		this.attribute_name=record[1];
		this.attribute_value=record[2];
		this.lchg_time = insertTimeStr;
		this.batch_num = batch_num;
		
	}
	public String getitem()
	{
		return this.item;
	}
	public String getattribute_name()
	{
		return this.attribute_name;
	}
	public String getattribute_value()
	{
		return this.attribute_value;
	}
	public String getinsertTimeStr()
	{
		return this.lchg_time;
	}
	public int getbatch_num()
	{
		return this.batch_num;
	}

}
