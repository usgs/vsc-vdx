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
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.nio.ByteBuffer;
import java.util.Vector;
/**
 * A class to read SEISAN files, adapted from Fissures code, via SEISAN.java
 * 
 */
public class SEISAN
{
	protected final static Logger logger = Log.getLogger("gov.usgs.vdx.data.wave.SEISAN"); 
	
	//line1
	public String networkName;
	public static int channel_number;
	public int[] channel_long;
	
	public long year;
	public String DOY;
	public long month;
	public long day;
	public long hour;
	public long minute;
	public float second;
	public float total_Time_Window_persec;
	private int channel_indice = 0;
	
	
	
	//line2 is free
	
	
	// line 3
	
	public String station_CodeA4;
	public static String FIRST_two_COMPONENT_CODES;
	public String LAST_COMPONENT_CODE;
	public String STATION_CODE;
	public float Start_Time;
	public float Station_Data_Interval_Length;
	public int data_size;
	
	
	 //EVENT FILE CHANNEL HEADER VARIABLES

	public static String STATION_CODe;
	public String FIRST_TWO_COMPONENT_CODES;
	public String FIRST_LOCATION_CODE;
	public String last_component_code;
	public int YEAR;
	public String SECOND_LOCATION_CODE;
	public int doy;
	public String FIRST_NETWORK_CODE;
	public int MONTH;
	public String SECOND_NETWORK_CODE;
	public int DAY;
	public int hours;
	public int minutes;
	public String TIMING_INDICATOR;  //BLANK: TIME IS OK, E: UNCERATIAN TIME
	public float SECOND ;
	public float SAMPLE_RATE;
	public int NUMBER_OF_SAMPLES;
	public float LATITUDE ;
	public float LONGITUDE ;
	public float ELEVATION ;
	public String gain_factor;  //Blank: No gain factor, G: Gain factor in   column 148 to 159
	public static String[] stationCode;
	public static String[] component;

	public float[] y;
	public static Vector<Integer> in_buf;
	public static Vector<Integer> in_buf_test;
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

	// undef values for SEISAN
	public static float FLOAT_UNDEF = -12345.0f;
	public static int INT_UNDEF = -12345;
	public static String STRING8_UNDEF = "-12345";
	public static String STRING16_UNDEF = "-12345";

	/* TRUE and FALSE defined for convenience. */
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	int stationInfoIndice = 0;

	// foe testing little or big endien
	public enum FileTypeCheck {
		LittleEndienn, BigEndienn,UNKNOWN;

		public static FileTypeCheck checkType(DataInputStream in) throws IOException {
		
			FileTypeCheck result = null ;
			byte[] byte4   = new byte[4];

			in.read(byte4);
			System.out.println(byte4[0]+" "+byte4[1]+" "+byte4[2]+" "+byte4[3]);
			
			if( byte4[0] ==80 && byte4[1] ==0  && byte4[2] ==0 && byte4[3] == 0){
				in.skipBytes(84);
				in.read (byte4);
				
				if( byte4[0] ==80 && byte4[1] ==0  && byte4[2] ==0 && byte4[3] == 0){
					 result = LittleEndienn;
				 }
			}else if ( byte4[0] ==0 && byte4[1] ==0  && byte4[2] ==0 && byte4[3] == 80){
				in.skipBytes(84);
				in.read (byte4);
				
				if( byte4[0] ==0 && byte4[1] ==0  && byte4[2] ==0 && byte4[3] == 80){
					 result = BigEndienn;
				 }
			}else 
				result = UNKNOWN;
			return result;
			
			
			
			
			
		}
	}
	
	public static int getChannelNumber(){
		return channel_number;
	}
	
	/* Constants used by SEISAN. */

	public SEISAN()
	{	
		in_buf = new Vector<Integer>();	
	}
	
	/** reads the SEISAN file specified by the filename. 
	 * @param filename file's name
	 * @throws FileNotFoundException if the file cannot be found
	 * @throws IOException if it isn't a WIN file or if it happens :)
	 */
	public void read(String filename) throws FileNotFoundException, IOException
	{
		
		FileInputStream fis = new FileInputStream(filename);
		BufferedInputStream buf = new BufferedInputStream(fis);
		DataInputStream dis = new DataInputStream(buf);
	
		
		in_buf_test = new Vector<Integer>();
		//DataInputStream in = dis;
		// for the endiann verification , 
		//System.out.println(FileTypeCheck.checkType(in));
		////////* for testing if there are more data avaible *///////
		
		readHeader(dis);
		channel_long = new int[channel_number];
		 stationCode = new String[channel_number];
		component = new String[channel_number];
		
		for (int i =0 ; i<3 ; i++)
		{
			
			readHeaderChannel1(dis);
			readData(dis);
		}
		byte[] b1 = new byte[1];
		String skip = null;
		int compt = 0;
		
		// i skipped the additionnal data that i have found on the test file 
		for (int i =0 ; i<879 ; i++)
		{
			
			dis.read(b1);
			skip = new String (b1);
			compt++;
			//System.out.print(chenker);
			
		}
		
		
		
		readHeaderChannel1(dis);
		readData(dis);
		
		for (int i =0 ; i<channel_number - 4 ; i++)
		{
			
			readHeaderChannel1(dis);
			readData(dis);
		}
		
		
		
System.out.println(skip);
//System.out.println(compt);		
		dis.close();
		stationInfoIndice = 0;
	}
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
	
	public Wave toWavetest(int num_wave)
	{
		int dataSkip = 0;
		Wave sw = new Wave();
		Wave sw1 = new Wave();
		//sw.setStartTime(Util.dateToJ2K(getStartTime()));
		sw.setSamplingRate(getSamplingRate());
		sw.buffer = new int[channel_long[num_wave]];
		for (int j=1;j<num_wave;j++){
			dataSkip+=channel_long[j];
		}
		int j = 0;
		for (int i = dataSkip; i <dataSkip+ channel_long[num_wave]; i++)
		{
			
			
			sw.buffer[j] = Math.round(in_buf_test.elementAt(i));
			j++;
		
		}

		return sw;
	}
	
	public double getSamplingRate()
	{
		return SAMPLE_RATE;
	}
	
	
	public String getStationCode(int channelNum)
	{
		return stationCode[channelNum];
	}
	
	public String getComponent(int channelNum)
	{
		return component[channelNum];
	}
	
	
	
	
	
	public static String getStationInfo()
	{
		return "theseisanfile";
	}

	
	private static int intFromFourBytes (byte[] bites)
	{
		ByteBuffer wrapped = ByteBuffer.wrap(bites); 
		return wrapped.getInt ();
	}
	
	public void readData(DataInputStream dis) throws IOException
	{
		
		int bytesRead = 0;
		
		do 
		{	
			byte[] fourBytes = new byte[4];
			
		int accum = 0 ;
			try {
				dis.readFully(fourBytes);
			 accum = intFromFourBytes (fourBytes);
			}
			catch (Exception ex){
				System.out.println(ex.getMessage());
			}
			baseVal = accum;
				
			
			System.out.println ("base: " + accum);
	
		float[] d = new float[NUMBER_OF_SAMPLES - 1];
			if (data_size == 1)
			{
				for (int ix = 0; ix < (NUMBER_OF_SAMPLES - 1); ix++)
				{		
					accum += dis.readInt ();
					in_buf.add(accum);
					bytesRead += 4;
				}	
			
			}
		
		
			else if (data_size == 4)
			{
				int ix;
				for ( ix = 0; ix < (NUMBER_OF_SAMPLES-1 ); ix++)
				{		
					
					try {
						dis.read (fourBytes);
					}catch (Exception ex){
						System.out.println(ex.getMessage());
					}
					accum += intFromFourBytes (fourBytes);
					in_buf.add (accum);
					in_buf_test.add(accum);
					d[ix] = accum;
					bytesRead += 4;
					//System.out.println ("data bytes: " + d[ix]);
				}
				
				channel_long[channel_indice++]= ix+1;
			
			}
	//in_buf_test.add(0000000000);
		
			
		} while (bytesRead <= SAMPLE_RATE);
		
		System.out.println ("DONE");
		//System.out.println(dis.readLine());
		
	}	

	
	
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

	private void readHeaderChannel1(DataInputStream dis) throws IOException {
		byte[] byte4 = new byte[4];
		byte[] byte7 = new byte[7];
		byte[] byte8 = new byte[8];
		byte[] byte9 = new byte[9];
		byte[] byte5 = new byte[5];
		byte[] byte2 = new byte[2];
		byte[] byte1 = new byte[1];
		byte[] byte3 = new byte[3];
		byte[] byte6 = new byte[6];
		byte[] byte80 = new byte[80];
		byte[] byte800 = new byte[800];
		
		
		dis.skip(8);
		dis.read(byte5);

		STATION_CODe= new String (byte5).trim();
		dis.read(byte2);
		FIRST_TWO_COMPONENT_CODES= new String (byte2).trim();
		dis.read(byte1);
		FIRST_LOCATION_CODE= new String (byte1).trim();
		dis.read(byte1);
		last_component_code= new String (byte1).trim();
		dis.read(byte3);
		YEAR=1900+ Integer.parseInt(new String (byte3).trim());
		dis.read(byte1);
		SECOND_LOCATION_CODE =new String (byte1).trim();
		dis.read(byte3);
		doy= Integer.parseInt(new String (byte3).trim());
		dis.read(byte1);
		FIRST_NETWORK_CODE =new String (byte1).trim();
		dis.read(byte2);
		MONTH= Integer.parseInt(new String (byte2).trim());
		dis.read(byte1);
		SECOND_NETWORK_CODE =new String (byte1).trim();
		dis.read(byte2);
		DAY= Integer.parseInt(new String (byte2).trim());
		dis.skip(1);
		dis.read(byte2);
		hours= Integer.parseInt(new String (byte2).trim());
		dis.skip(1);
		dis.read(byte2);
		minutes= Integer.parseInt(new String (byte2).trim());
		dis.read(byte1);
		TIMING_INDICATOR =new String (byte1).trim();
		dis.read(byte6);
		SECOND= Float.parseFloat(new String (byte6).trim());
		dis.skip(1);
		dis.read(byte7);
		SAMPLE_RATE= Float.parseFloat(new String (byte7).trim());
		dis.read(byte7);
		NUMBER_OF_SAMPLES= Integer.parseInt(new String (byte7).trim());
		dis.skip(1);
		dis.read(byte8);
		// here we have some empty string Values !
		
		//LATITUDE = Float.parseFloat(new String (byte8).trim());
		dis.skip(1);
		dis.read(byte9);
		//LONGITUDE= Float.parseFloat(new String (byte9).trim());
		dis.skip(1);
		dis.read(byte5);
		//ELEVATION= Integer.parseInt(new String (byte5).trim());
		dis.read(byte1);
		gain_factor =new String (byte1).trim();
		dis.read(byte1);
		data_size = Integer.parseInt(new String (byte1).trim());
		// here i m skipping all the rest of the header cause it s blanc on the file 
		dis.skip(963);
		//System.out.println(dis.readLine());
		dis.skip(8);
		
		stationCode[stationInfoIndice]= STATION_CODe;
		component[stationInfoIndice]=FIRST_TWO_COMPONENT_CODES + FIRST_LOCATION_CODE;
		stationInfoIndice++;
		//dis.skip(60000);
		//System.out.println(dis.readLine());
		System.out.println(" ***EVENT FILE CHANNEL HEADER Info*** : STATION_CODe :"+STATION_CODe+"   ,FIRST_TWO_COMPONENT_CODES:  "+FIRST_TWO_COMPONENT_CODES+"   ,FIRST_LOCATION_CODE:"+FIRST_LOCATION_CODE+"   ,last_component_code:"+last_component_code+"   ,YEAR:"+YEAR+"   ,SECOND_LOCATION_CODE;"+SECOND_LOCATION_CODE+"   ,doy:"+doy+"   ,FIRST_NETWORK_CODE:"+FIRST_NETWORK_CODE+"   ,MONTH:"+MONTH+"   ,SECOND_NETWORK_CODE:"+SECOND_NETWORK_CODE+"   ,DAY:"+DAY+"   ,hours:"+hours+"   ,minutes:"+minutes+"   ,TIMING_INDICATOR:"+TIMING_INDICATOR+"   ,SECOND:"+SECOND+"   ,SAMPLE_RATE:"+SAMPLE_RATE+"   ,NUMBER_OF_SAMPLES:"+NUMBER_OF_SAMPLES+"   ,LATITUDE:"+LATITUDE+"   ,LONGITUDE:"+  LONGITUDE+"  ,ELEVATION:"+ELEVATION+"   ,gain_factor:"+gain_factor+"   ,data_size: "+data_size);	
	}

	public void readHeader(DataInputStream dis) throws FileNotFoundException, IOException
	{
		dis.skip(5);
		byte[] byte29 = new byte[29];
		byte[] byte7 = new byte[7];
		byte[] byte2 = new byte[2];
		byte[] byte6 = new byte[6];
		byte[] byte9 = new byte[9];
		byte[] byte4 = new byte[4];
		byte[] byte1 = new byte[1];
		byte[] byte8 = new byte[8];
		
		dis.read(byte29);
		networkName  = new String(byte29).trim();
		
		byte[] byte3 = new byte[3];
		dis.read(byte3);
		channel_number = Integer.parseInt(new String (byte3).trim());
		//ByteBuffer bb = ByteBuffer.wrap(byte3);
		// int serialNumber = 
			//        ByteBuffer.wrap(byte3).order(ByteOrder.LITTLE_ENDIAN).getInt();
		  //int numberChannel = ( byte3[2] << 16) |(byte3[1] << 8)  | byte3[0];         
		dis.read(byte3);
		//int year = ( byte3[2] << 16) |(byte3[1] << 8)  | byte3[0];
		 year = Integer.parseInt(new String (byte3).trim()) + 1900;
		
		dis.skip(1);
		dis.read(byte3);
		DOY = new String (byte3);
		
		dis.skip(1);
		dis.read(byte2);
		 month = Integer.parseInt(new String (byte2).trim());
		dis.skip(1);
		dis.read(byte2);
		 day = Integer.parseInt(new String (byte2).trim());
		dis.skip(1);
		dis.read(byte2);
		 hour = Integer.parseInt(new String (byte2).trim());
		 dis.skip(1);
			dis.read(byte2);
			 minute = Long.parseLong(new String (byte2).trim());
			 dis.skip(1);
				dis.read(byte6);
				 second = Float.parseFloat(new String (byte6).trim());
				 dis.skip(1);
					dis.read(byte9);
					 total_Time_Window_persec = Float.parseFloat(new String (byte9).trim());
					 
					 System.out.println("the first line of the seisan file ////// network name :"+networkName+"   ,channel number:"+channel_number+"  ,year:"+year+"   ,DOY:"+DOY+"   ,month:"+month+"   ,day:"+day+"   ,hours:"+hour+"   ,minutes:"+minute+"  ,second:"+second+"  ,total time winodow:"+total_Time_Window_persec);
		dis.skip(15);
		//we skip the second line because it's a blanc line 
		dis.skip(88);
		
		//parsing the 3rd line 
		
		dis.skip(5);
		dis.read(byte4);
		station_CodeA4 = new String (byte4).trim();
		// for testing with files 2003
		if(station_CodeA4 == null){
			dis.skip(79);
		}else{
		dis.read(byte2);
		FIRST_two_COMPONENT_CODES = new String (byte2).trim();
		dis.skip(1);
		dis.read(byte1);
		LAST_COMPONENT_CODE = new String(byte1).trim();
		
		
		dis.read(byte1);
		STATION_CODE = new String (byte1).trim();
		dis.read(byte7);
		
		Start_Time = Float.parseFloat(new String (byte7).trim());
		dis.skip(1);
		dis.read(byte8);
		Station_Data_Interval_Length = Float.parseFloat(new String (byte8));
		
		System.out.println("the thrid line ://// station Code  A4: "+station_CodeA4+"  ,FIRST_two_COMPONENT_CODES: "+FIRST_two_COMPONENT_CODES+" ,LAST_COMPONENT_CODE:  "+LAST_COMPONENT_CODE+"  ,Station code :"+STATION_CODE+"  ,start time:"+Start_Time+"  ,Station_Data_Interval_Length :"+Station_Data_Interval_Length);
		// i skipped second and third channel for verification of their use
		dis.skip(58);}
		
		//reading the fourth line :
		
		dis.skip(5);
		dis.read(byte4);
		station_CodeA4 = new String (byte4).trim();
		if(station_CodeA4 == null){
			dis.skip(79);
		}else{
		dis.read(byte2);
		FIRST_two_COMPONENT_CODES = new String (byte2).trim();
		dis.skip(1);
		dis.read(byte1);
		LAST_COMPONENT_CODE = new String(byte1).trim();
		
		
		dis.read(byte1);
		STATION_CODE = new String (byte1).trim();
		dis.read(byte7);
		dis.skip(1);
		Start_Time = Float.parseFloat(new String (byte7).trim());
		dis.read(byte8);
		Station_Data_Interval_Length = Float.parseFloat(new String (byte8));
		
		System.out.println("the fourth line ://// station Code  A4: "+station_CodeA4+"  ,FIRST_two_COMPONENT_CODES: "+FIRST_two_COMPONENT_CODES+" ,LAST_COMPONENT_CODE:  "+LAST_COMPONENT_CODE+"  ,Station code :"+STATION_CODE+"  ,start time:"+Start_Time+"  ,Station_Data_Interval_Length :"+Station_Data_Interval_Length);
		// i skipped second and third channel for verification of their use
				dis.skip(58);}

	//reading the fifth line :
		
		dis.skip(5);
		dis.read(byte4);
		station_CodeA4 = new String (byte4).trim();
		if(station_CodeA4 == null){
			dis.skip(79);
		}else{
		dis.read(byte2);
		FIRST_two_COMPONENT_CODES = new String (byte2).trim();
		dis.skip(1);
		dis.read(byte1);
		LAST_COMPONENT_CODE = new String(byte1).trim();
		
		
		dis.read(byte1);
		STATION_CODE = new String (byte1).trim();
		dis.read(byte7);
		dis.skip(1);
		Start_Time = Float.parseFloat(new String (byte7).trim());
		dis.read(byte8);
		Station_Data_Interval_Length = Float.parseFloat(new String (byte8));
		
		System.out.println("the fifth line ://// station Code  A4: "+station_CodeA4+"  ,FIRST_two_COMPONENT_CODES: "+FIRST_two_COMPONENT_CODES+" ,LAST_COMPONENT_CODE:  "+LAST_COMPONENT_CODE+"  ,Station code :"+STATION_CODE+"  ,start time:"+Start_Time+"  ,Station_Data_Interval_Length :"+Station_Data_Interval_Length);
		// i skipped second and third channel for verification of their use
				dis.skip(58);}
		
	//reading the six line :
		
		dis.skip(5);
		dis.read(byte4);
		station_CodeA4 = new String (byte4).trim();
		if(station_CodeA4 == null){
			dis.skip(79);
		}else{
		dis.read(byte2);
		FIRST_two_COMPONENT_CODES = new String (byte2).trim();
		dis.skip(1);
		dis.read(byte1);
		LAST_COMPONENT_CODE = new String(byte1).trim();
		
		
		dis.read(byte1);
		STATION_CODE = new String (byte1).trim();
		dis.read(byte7);
		dis.skip(1);
		Start_Time = Float.parseFloat(new String (byte7).trim());
		dis.read(byte8);
		Station_Data_Interval_Length = Float.parseFloat(new String (byte8));
		
		System.out.println("the six line ://// station Code  A4: "+station_CodeA4+"  ,FIRST_two_COMPONENT_CODES: "+FIRST_two_COMPONENT_CODES+" ,LAST_COMPONENT_CODE:  "+LAST_COMPONENT_CODE+"  ,Station code :"+STATION_CODE+"  ,start time:"+Start_Time+"  ,Station_Data_Interval_Length :"+Station_Data_Interval_Length);
		// i skipped second and third channel for verification of their use
		dis.skip(58);}
		
		// skipping the blanc lines 
		dis.skip(524);
	}
	
	/** just for testing. Reads the filename given as the argument,
	 *  writes out some header variables and then
	 *  writes it back out as "outWINfile".
	 * @param args command line args
	 */
	public static void main(String[] args)
	{
		SEISAN data = new SEISAN();

		if (args.length != 1)
		{
			System.out.println("Usage: java gov.usgs.vdx.data.wave.WIN WINsourcefile ");
			//return;
		}

		try
		{
			data.read(args[0]);
			
			//System.out.println(Util.formatDate(data.getStartTime()));
			System.out.println(data.getSamplingRate());
			System.out.println(in_buf);
			System.out.println(in_buf_test);
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