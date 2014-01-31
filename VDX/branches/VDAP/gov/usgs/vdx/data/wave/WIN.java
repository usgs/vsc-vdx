/*
 * This code is derived from:
 * 
 The TauP Toolkit: Flexible Seismic Travel-Time and Raypath Utilities.
 Copyright (C) 1998-2000 University of South Carolina

 This program is free software; you can redistribute it and/or
 modify it under the terms of the GNU General Public License
 as published by the Free Software Foundation; either version 2
 of the License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.

 The current version can be found at
 <A HREF="www.seis.sc.edu">http://www.seis.sc.edu</A>

 Bug reports and comments should be directed to
 H. Philip Crotwell, crotwell@seis.sc.edu or
 Tom Owens, owens@seis.sc.edu
 */

package gov.usgs.vdx.data.wave;

import gov.usgs.util.Log;
import gov.usgs.util.Util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.nio.ByteBuffer;
import java.util.Vector;
/**
 * A class to read WIN files, adapted from Fissures code, via WIN.java
 * 
 */
public class WIN
{
	protected final static Logger logger = Log.getLogger("gov.usgs.vdx.data.wave.WIN"); 

	public int packetSize;	
	public long year;
	public long month;
	public long day;
	public long hour;
	public long minute;
	public long second;
	public int channel_num;
	public int data_size;
	public float sampling_rate; 
	public float[] y;
	public Vector<Integer> in_buf;
	int yLen;
	public float[] x;
	public float[] real;
	public float[] imaginary;
	public float[] amp;
	public float[] phase;
	int baseVal;
	
	public String kstnm = STRING8_UNDEF;  //
	public String kcmpnm = STRING8_UNDEF;
	public String knetwk = STRING8_UNDEF;

	// undef values for WIN
	public static float FLOAT_UNDEF = -12345.0f;
	public static int INT_UNDEF = -12345;
	public static String STRING8_UNDEF = "-12345  ";
	public static String STRING16_UNDEF = "-12345          ";

	/* TRUE and FALSE defined for convenience. */
	public static final int TRUE = 1;
	public static final int FALSE = 0;

	/* Constants used by WIN. */

	public WIN()
	{	
		in_buf = new Vector<Integer>();	
	}
	
	/** reads the WIN file specified by the filename. 
	 * @param filename file's name
	 * @throws FileNotFoundException if the file cannot be found
	 * @throws IOException if it isn't a WIN file or if it happens :)
	 */
	public void read(String filename) throws FileNotFoundException, IOException
	{
		File WINFile = new File(filename);
	//	in_buf = new Vector<Integer> ();
		FileInputStream fis = new FileInputStream(filename);
		BufferedInputStream buf = new BufferedInputStream(fis);
		DataInputStream dis = new DataInputStream(buf);
		while (dis.available() != 0)
		{
			readHeader(dis);
			readData(dis);
		}	
	
		dis.close();
	}

	

	/**
	 * reads the WIN from DataInputStream
	 * @param dis DataInputStream to read WIN from
	 * @throws IOException if can't read from input stream
	 */
	public void read(DataInputStream dis) throws IOException
	{
		while (dis.available() != 0)
		{
			readHeader(dis);
			readData(dis);
		}	
	}
/** reads just the WIN header specified by the filename. 
	 * @param filename file's name
	 * @throws FileNotFoundException if the file cannot be found
	 * @throws IOException if it isn't a WIN file 
	 */
	
	public void readHeader(String filename) throws FileNotFoundException, IOException
	{
		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(filename)));
		readHeader(dis);
		dis.close();
		
	}

	static private int intFromSingleByte (byte b)
	{
		return b;
	}
	private static int intFromFourBytes (byte[] bites)
	{
		ByteBuffer wrapped = ByteBuffer.wrap(bites); 
		return wrapped.getInt ();
	}
	
	private static int intFromThreeBytes (byte[] bites)
	{
        // TODO: not sure about the endianness here
        byte[] padded = new byte[] { 0, bites[2], bites[1], bites[0] };
		ByteBuffer wrapped = ByteBuffer.wrap(padded);
		return wrapped.getInt ();
	}
	
	private static short shortFromTwoBytes (byte[] bites)
	{
		ByteBuffer wrapper = ByteBuffer.wrap(bites);
		return wrapper.getShort();
	
	}
	
	private static int decodeBcd(byte[] b) 
	{
	    StringBuffer buf = new StringBuffer(b.length * 2);
	    for (int i = 0; i < b.length; ++i) {
	        buf.append((char) (((b[i] & 0xf0) >> 4) + '0'));
	        if ((i != b.length) && ((b[i] & 0xf) != 0x0A)) 
	            buf.append((char) ((b[i] & 0x0f) + '0'));
	    }
	    return Integer.parseInt(buf.toString());
	}   
	/** reads the header from the given stream.	 
	 * @param dis DataInputStream to read WIN from
	 * @throws FileNotFoundException if the file cannot be found
	 * @throws IOException if it isn't a WIN file 
	 */
	public void readHeader(DataInputStream dis) throws FileNotFoundException, IOException
	{
		//read first 4 byte: file size
		byte[] fourBytes = new byte[4];
		byte[] oneByte   = new byte[1];
		
		dis.readFully(fourBytes);	
		packetSize = intFromFourBytes (fourBytes);
	
		
		//read next 6 bytes: yy mm dd hh mi ss
		dis.readFully (oneByte);
		year = 2000 + decodeBcd(oneByte);
		
		dis.readFully(oneByte);
		month = decodeBcd(oneByte);
		
		dis.readFully(oneByte);
		day = decodeBcd(oneByte);
		
		dis.readFully(oneByte);
		hour = decodeBcd(oneByte);
		
		dis.readFully(oneByte);
		minute = decodeBcd(oneByte);
		
		dis.readFully(oneByte);
		second = decodeBcd(oneByte);
		
		/*byte[] fourBytes1   = new byte[4];
		kstnm = new String(fourBytes1);
		dis.readFully(fourBytes1);
		kcmpnm = new String(fourBytes1);
		dis.readFully(fourBytes1);

		
		//here to get the header from the win files 
		
		/*dis.readFully(sexteenByttes);
		kstnm = new String(sexteenByttes);
		dis.readFully(sexteenByttes);
		kcmpnm = new String(sexteenByttes);
		dis.readFully(sexteenByttes);
		knetwk = new String(sexteenByttes);*/
		System.out.println ("******************************************\npacket size: " + packetSize + "\tyear " + year + "\tmonth: " + month 
					+ "\tday: " +  day + "\thour: " + hour + "\tminute: " +  minute + "\tsec: " + second );
	}

	/** read the data portion of WIN format from the given stream
	 * @param fis DataInputStream to read WIN from
	 * @throws IOException if it isn't a WIN file
	 */
	public void readData(DataInputStream dis) throws IOException
	{
		
		int bytesRead = 10;
		
		do 
		{	
			byte[] oneByte = new byte[1];
			dis.readFully(oneByte);
			float some_code = intFromSingleByte(oneByte[0]);
		
			dis.readFully(oneByte);
			channel_num = intFromSingleByte(oneByte[0]);		
		
			dis.readFully(oneByte);
			data_size = intFromSingleByte(oneByte[0])>>4;
		
			dis.readFully(oneByte);
			sampling_rate = intFromSingleByte(oneByte[0]); // TODO: needs lower 4 bits of above byte included
			System.out.println ("channel num: " + channel_num + " sampling rate: " 
					+ sampling_rate);	
		
			//int ix = 0;
			byte[] fourBytes = new byte[4];
			dis.readFully(fourBytes);
			int accum = intFromFourBytes (fourBytes);
			baseVal = accum;
				
			float[] d = new float[(int) sampling_rate - 1];
			System.out.println ("base: " + accum);
	
			bytesRead += 8;
            if (data_size == 0) {
                throw new RuntimeException("sample size of 0 is unimplemented");
            }
			else if (data_size == 1)
			{
				for (int ix = 0; ix < ((int) sampling_rate - 1); ix++)
				{
					accum += dis.readByte();
					in_buf.add(accum);
					bytesRead ++;
				}	
			}
			else if (data_size == 2)
			{
				byte[] twoBytes = new byte[2];
				for (int ix = 0; ix < ((int) sampling_rate - 1); ix++)
				{		
					dis.readFully(twoBytes);
					accum += shortFromTwoBytes (twoBytes);
					d[ix] = accum;
					in_buf.add (accum);
					System.out.println ("data bytes: " + d[ix]);
					bytesRead += 2;
				}	
			}
			else if (data_size == 3)
			{
				byte[] threeBytes = new byte[3];
				for (int ix = 0; ix < ((int) sampling_rate - 1); ix++)
				{		
				
					dis.readFully(threeBytes);
					accum += intFromThreeBytes (threeBytes);
					d[ix] = accum;
					bytesRead += 3;
				}	
			}
			else if (data_size == 4)
			{
				byte[] four_bytes = new byte[4];
				for (int ix = 0; ix < ((int) sampling_rate - 1); ix++)
				{		
					dis.readFully (fourBytes);
					accum += intFromFourBytes (fourBytes);
					
					d[ix] = accum;
					bytesRead += 4;
				}	
			}
	
			yLen += packetSize;
			
		} while (bytesRead < packetSize);
		
		System.out.println ("DONE");
		
		
	}	

	
	/** writes this object out as a WIN file.
	 * @param filename name of file to read WIN from
	 * @throws FileNotFoundException if the file cannot be found
	 * @throws IOException if it isn't a WIN file 
 	 */
	public void write(String filename) throws FileNotFoundException, IOException
	{
		File f = new File(filename);
		write(f);
	}

	/** writes this object out as a WIN file.
	 * @param file file to write to
	 * @throws FileNotFoundException if the file cannot be found
	 * @throws IOException if it isn't a WIN file 
	 */
	public void write(File file) throws FileNotFoundException, IOException
	{
		DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
		writeHeader(dos);
		writeData(dos);
		dos.close();
	}


	/**
	 * Write WIN header to output stream
	 * @param dos DataOutputStream to write toWave
	 * @throws IOException if writing fails
	 */
	public void writeHeader(DataOutputStream dos) throws IOException
	{
	dos.writeBytes("the header");
	/*if (kstnm.length() > 4)
	{
		kstnm = kstnm.substring(0, 3);
	}
	while (kstnm.length() < 4)
	{
		kstnm += " ";
	}
	dos.writeBytes(kstnm);
	
	if (kcmpnm.length() > 4)
	{
		kcmpnm = kcmpnm.substring(0, 3);
	}
	while (kcmpnm.length() < 4)
	{
		kcmpnm += " ";
	}
	dos.writeBytes(kcmpnm);
	
	if (knetwk.length() > 4)
	{
		knetwk = knetwk.substring(0, 3);
	}
	while (knetwk.length() < 4)
	{
		knetwk += " ";
	}
	dos.writeBytes(knetwk);*/
	
	}

	/**
	 * Write data portion of WIN format in the given output stream
	 * @param dos DataOutputStream to write toWave
	 * @throws IOException if writing fails
	 */
	public void writeData(DataOutputStream dos) throws IOException
	{

	}

	/**
	 * Print header to stdout
	 */
	public void printHeader()
	{
	
	}

	/**
	 * Create Wave object from WIN data
	 * @return wave created
	 */
	public Wave toWave()
	{
		Wave sw = new Wave();
		//sw.setStartTime(Util.dateToJ2K(getStartTime()));
		sw.setSamplingRate(getSamplingRate());
		sw.buffer = new int[in_buf.size()];
		for (int i = 0; i < in_buf.size(); i++)
		{
			sw.buffer[i] = Math.round(in_buf.elementAt(i));
		}

		return sw;
	}

	public String getStationInfo()
	{
		//return kstnm.trim() + "_" + kcmpnm.trim() + "_" + knetwk.trim();
		return "test win file";
	}

	/**
	 * Get start time of data
	 * @return start time of data
	 */
	public Date getStartTime()
	{
		String ds = year + "," + day + "," + hour + "," + minute + "," + second + ",0.0";
		SimpleDateFormat format = new SimpleDateFormat("yyyy,DDD,HH,mm,ss,SSS");
		format.setTimeZone(TimeZone.getTimeZone("GMT"));
		Date d = null;
		try
		{
			d = format.parse(ds);
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
		return d;
	}

	/**
	 * Get sample rate
	 * @return sample rate
	 */
	public double getSamplingRate()
	{
		return sampling_rate;
	}

	/** just for testing. Reads the filename given as the argument,
	 *  writes out some header variables and then
	 *  writes it back out as "outWINfile".
	 * @param args command line args
	 */
	public static void main(String[] args)
	{
		WIN data = new WIN();

		if (args.length != 1)
		{
			System.out.println("Usage: java gov.usgs.vdx.data.wave.WIN WINsourcefile ");
			//return;
		}

		try
		{
			data.read(args[0]);
			data.printHeader();
			System.out.println(Util.formatDate(data.getStartTime()));
			System.out.println(data.getSamplingRate());
			Wave sw = data.toWave();
			System.out.println(sw);
		}
		catch (FileNotFoundException e)
		{
			System.out.println("File " + args[0] + " doesn't exist.");
		}
		catch (IOException e)
		{
			System.out.println("IOException: " + e.getMessage());
		}
	}
}