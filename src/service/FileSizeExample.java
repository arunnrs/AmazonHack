package service;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;

import static common.Constants.*;
public class FileSizeExample 
{
    public static void readWrite() throws IOException {
    }
    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        //InputStream out = new BufferedInputStream(new FileInputStream(new File(FILE1+BLOCK)));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                        
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }
	public static void main(String[] args) throws IOException
    {	
		System.out.println(countLines(FILE1));
		int maxLines=5;
		long linesCount=countLines(FILE1);
		long numOfSplit=linesCount/maxLines;
		long pendingLines=0;
		String line = null;
		int count=0;
		int fileCount=1;
		PrintWriter pw=new PrintWriter(FILE1+fileCount);
		if(numOfSplit!=0)
		{
			pendingLines = linesCount%10000;

				BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(FILE1))));
		        while ((line = br.readLine()) != null)
		        {	
		        	if(count==maxLines)
		        	{
		        		pw.flush();
		        		pw.close();
		        		fileCount++;
		        		pw = new PrintWriter(FILE1+fileCount);
		        		count=0;
		        	}
		        	count++;
		        	System.out.println(line);
		        	pw.println(line);	
		        }
				pw.flush();
        		pw.close();
			}
	}
    }
	
  
		
		
		
		
 

