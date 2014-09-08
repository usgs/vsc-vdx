package gov.usgs.vdx.data.wave;


import gov.usgs.util.Util;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;


/**
 * 
 * Holds data representing a Seisan file. This includes channel headers and channel data
 * 
 * @author Chirag Patel
 *
 */
public class SeisanFile {
	
	private ArrayList<SeisanChannel> channels = new ArrayList<SeisanChannel>();
	private int noOfChannels;
	private Integer year;
	private Integer month;
	private Integer day;
	private Integer hour;
	private Integer minute;
	private Float second;
	private String networkName;
	private String fileName;
	private Date startDate;
	private FileFlag fileFlag;
	private int numberLength;
	
	
	/**
	 * Tries to read data from seisan file.
	 * <br />
	 * Function reads both headers and channel header and data for wave file.
	 * 
	 * 
	 * @param filename
	 * @throws IOException
	 */
	public void read(String filename) throws IOException{
		fileName = filename;
		FileInputStream fis = new FileInputStream(filename);
		BufferedInputStream buf = new BufferedInputStream(fis);
		DataInputStream dis = new DataInputStream(buf);
		detectParameters(dis);
		String data  = readFileHeader(dis,80);
//		System.out.println(data);
		noOfChannels = Integer.parseInt(data.substring(30,33).trim());

		networkName = data.substring(1, 30).trim();
		year =  data.substring(33,36).trim().length() == 0?null:Integer.parseInt(data.substring(33,36).trim());
		month = data.substring(41,43).trim().length() == 0?null:Integer.parseInt(data.substring(41,43).trim());
		day  =  data.substring(44,46).trim().length() == 0?null:Integer.parseInt(data.substring(44,46).trim());
		hour  = data.substring(48,49).trim().length() == 0?null:Integer.parseInt(data.substring(48,49).trim());
		minute  = data.substring(50,52).trim().length() == 0?null:Integer.parseInt(data.substring(50,52).trim());
		second  = data.substring(53,59).trim().length() == 0?null:Float.parseFloat(data.substring(53,59).trim());

		Calendar c  = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		c.setTimeInMillis(0);
		year = year+1900;
		c.set(Calendar.YEAR, (year));
		c.set(Calendar.MONTH, month-1);
		c.set(Calendar.DAY_OF_MONTH, day);
		c.set(Calendar.HOUR_OF_DAY, hour);
		c.set(Calendar.MINUTE, minute);
		c.set(Calendar.SECOND, second.intValue());
		startDate= c.getTime();
		
	    System.out.println("FILE HEADER: No Of Channels:"+noOfChannels+",Date:"+c.getTime()+",Start Date:"+startDate);
	    
	    int number_of_lines = (noOfChannels/ 3) + (noOfChannels % 3 + 1);
	    
	    if (number_of_lines < 10){
	        number_of_lines = 10;
	    }
//	    System.out.println("Ignoring line:");
	    System.out.println(readLine(dis));
	    for (int i=0; i<number_of_lines; i++) {
//		    System.out.println("Ignoring line:");
		    System.out.println(readLine(dis));
	    }
	    
	    for(int i = 0 ; i < noOfChannels; i++){
	    	readChannel(dis);
	    }
	    dis.close();
	}
	

	/**
	 * Reads a single channel from the InputStream and adds it to the list of channels for this seisan file.
	 * 
	 * @param dis
	 * @throws IOException
	 */
	private void readChannel(DataInputStream dis) throws IOException{
		String channelHeader = readLine(dis);
		System.out.println("channel header: "+channelHeader);
		SeisanChannel channel = new SeisanChannel(channelHeader);
		readChannelData(dis,channel);
		channels.add(channel);
	}

	
	/**
	 * 
	 * reads channel wave data from the InputStream for the specified {@link SeisanChannel}
	 * 
	 * @param dis
	 * @param channel
	 * @throws IOException
	 */
	private void readChannelData(DataInputStream dis, SeisanChannel channel) throws IOException{
		int length = readInt(dis);
//		int[] wave_data = new int[length / numberLength];
		int[] wave_data = new int[length / 4];

		for (int i=0; i<wave_data.length; i++) {
//		    wave_data[i] = readInt(dis);
		    wave_data[i] = readInt4(dis);
		}
		int prevLength = readInt(dis);
		assert length == prevLength;
	    channel.setData(wave_data);
	}

	private int readInt(InputStream in) throws IOException {
		byte[] buf = new byte[numberLength];
		in.read(buf);
		return readInt(buf);
	}

	private int readInt4(InputStream in) throws IOException {
		byte[] buf = new byte[4];
		in.read(buf);
		return readInt(buf);
	}
	
	private int readInt(byte[] b) {
		ByteOrder endian = fileFlag == FileFlag.PC ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
		return b.length == 4 ? readInt4(b, endian) : readInt8(b, endian);
	}

	private ByteBuffer converter4 = ByteBuffer.wrap(new byte[4]);
	private ByteBuffer converter8 = ByteBuffer.wrap(new byte[8]);
	
	public int readInt4(byte[] b, ByteOrder endian) {
	    converter4.clear();
	    converter4.mark();
	    converter4.put(b);
	    converter4.rewind();
	    converter4.order(endian);
	    return converter4.getInt();
	}

	public int readInt8(byte[] b, ByteOrder endian) {
	    converter8.clear();
	    converter8.mark();
	    converter8.put(b);
	    converter8.rewind();
	    converter8.order(endian);
    	// Note, it's turned into an int at some point, so this truncation is assumed safe.
    	long test = converter8.getLong();
    	return (int)test;
	}

	/**
	 * Reads a single line with a specified length of information  from the InputStream
	 * @param dis
	 * @param length
	 * @return
	 * @throws IOException
	 */
	private String readLine(DataInputStream dis) throws IOException{
		int length = readInt(dis);
//		System.out.println("Reading line of length: "+length);
		byte[] bytes  = new byte[length];
	    dis.read(bytes);
		int prevLength = readInt(dis);
		assert length == prevLength;
	    return new String(bytes);
	}
	
	private String readFileHeader(DataInputStream dis, int length) throws IOException{
	    byte[] bytes = new byte[length];
	    dis.read(bytes);
		int prevLength = readInt(dis);
		assert length == prevLength;
	    return new String(bytes);
	}
	
	private void detectParameters(DataInputStream dis){
		byte[] bytes = new byte[4];
		try {
			dis.read(bytes);
			if (bytes[0] == 80) {
				fileFlag = FileFlag.PC;
			} else {
				fileFlag = FileFlag.SUN;
			}
			if (bytes[3] == 80) {
				numberLength = 4;
				return;
			}
			dis.mark(20);
			dis.read(bytes);
			if(bytes[0]==0){
				numberLength = 8;
			}else{
				dis.reset();
				numberLength = 4;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

	public ArrayList<SeisanChannel> getChannels() {
		return channels;
	}


	
	/**
	 * Return a {@link Wave} as a summation of all wave data for each SeisanChannel 
	 * 
	 * @return
	 */
	public Wave toWave()
	{
		Wave sw = new Wave();
		int size = 0;
		Float sampleRate = channels.get(0).getSampleRate();
		
		for(SeisanChannel c : channels){
			if(sampleRate >= c.getSampleRate()){
				sampleRate = c.getSampleRate();
			}
			size += c.getData().length;
		}
		sw.setStartTime(Util.dateToJ2K(startDate));
		sw.setSamplingRate(sampleRate);
		sw.buffer = new int[size];
		int index = 0;
		for (int i = 0; i < channels.size(); i++)
		{
			SeisanChannel c = channels.get(i);
			int[] data = c.getData();
			for(int j = 0; j < data.length; j++){
				sw.buffer[index] = data[j];
				index ++;
			}
			
		}

		return sw;
	}


	public String getStationInfo() {
		return (new File(fileName)).getName();
	}
	
	
}
