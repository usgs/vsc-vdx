package gov.usgs.vdx.data.wave;



import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;



public class SeisanFile {
	
	ArrayList<Channel> channels = new ArrayList<Channel>();
	
	String networkName;
	int noOfChannels;
	Integer year;
	String doy;
	Integer month;
	Integer day;
	Integer hour;
	Integer minute;
	Float second;
	String fileName;
	
	private Date d = new Date();
	private double startTime; 
	
	public void setDate(){
		d.setYear(year);
		d.setMonth(month - 1);
		d.setHours(hour);
		d.setDate(day);
		d.setMinutes(minute);
		d.setSeconds(0);
		d.setTime(d.getTime()+(int)(second * 1000));
	}
	
	
	public void read(String filename) throws IOException{
		fileName = filename;
		FileInputStream fis = new FileInputStream(filename);
		BufferedInputStream buf = new BufferedInputStream(fis);
		DataInputStream dis = new DataInputStream(buf);
		
		String data  = readLine(dis,80);
		noOfChannels = Integer.parseInt(data.substring(30,33).trim());
		
		year =  data.substring(33,36).trim().length() == 0?null:Integer.parseInt(data.substring(33,36).trim());
		month = data.substring(41,43).trim().length() == 0?null:Integer.parseInt(data.substring(41,43).trim());
		day  =  data.substring(44,46).trim().length() == 0?null:Integer.parseInt(data.substring(44,46).trim());
		hour  = data.substring(48,49).trim().length() == 0?null:Integer.parseInt(data.substring(48,49).trim());
		minute  = data.substring(50,52).trim().length() == 0?null:Integer.parseInt(data.substring(50,52).trim());
		second  = data.substring(53,59).trim().length() == 0?null:Float.parseFloat(data.substring(53,59).trim());

		
		setDate();
		startTime = d.getTime();
		
//		System.out.println("number of channels : "+ noOfChannels);
//		System.out.println();
//	    dis.skipBytes((5+channels)*88);
	    
	    
	    int number_of_lines = (noOfChannels/ 3) + (noOfChannels % 3 + 1);
	    
	    if (number_of_lines < 10){
	        number_of_lines = 10;
	    }
	    dis.skipBytes(88);
	    dis.skipBytes((number_of_lines)*88);
	    
	    for(int i = 0 ; i < noOfChannels; i++){
	    	readChannel(dis);
	    }
	    dis.close();
	}
	

	private void readChannel(DataInputStream dis) throws IOException{
		String channelHeader = readLine(dis,1040);
		Channel channel = new Channel(channelHeader);
//		System.out.println(channel);
//		System.out.println();
		
		
		readChannelData(dis,channel);
		
//		int offset = 4 * (channel.getNumberOfSamples() + 2);
//		byte[] data = new byte[offset];
//		dis.read(data);
//		
//		readChannelData(data,channel);
		
		channels.add(channel);
	}

	

	
	
	private void readChannelData(byte[] data, Channel channel) throws IOException{
		    int[] wave_data = new int[channel.getNumberOfSamples()];
		    
		   
			int index = 0;
		    for (int i = 3; i <= (channel.getNumberOfSamples() * 4); i += 4) {
				byte[] bytes = {data[i-3], data[i-2], data[i-1], data[i-0]};
				ByteBuffer wrapped = ByteBuffer.wrap(bytes); 
			    int val  = wrapped.getInt();
				wave_data[index] = val;
			    index ++;
			}
		    channel.setData(wave_data);
	}
	
	
	private void readChannelData(DataInputStream dis, Channel channel) throws IOException{
		dis.skipBytes(4);
		int[] wave_data = new int[channel.getNumberOfSamples()];
	    
	   
		int index = 0;
		while(index != channel.getNumberOfSamples()){
			byte[] bytes = new byte[4];
			dis.read(bytes);
//			int val = convert4(0, bytes);
			int val = byteArrayToInt(bytes);
		    wave_data[index] = val;
		   // System.out.println("wave data : " + val);
		    index ++;
		}	
//	    for (int i = 3; i <= (channel.getNumberOfSamples() * 4); i += 4) {
//			byte[] bytes = {data[i-3], data[i-2], data[i-1], data[i-0]};
//			ByteBuffer wrapped = ByteBuffer.wrap(bytes); 
//		    int val  = wrapped.getInt();
//			wave_data[index] = val;
//		    index ++;
//		}
		dis.skipBytes(4);
	    channel.setData(wave_data);
	    //dis.skipBytes(4);
	}
	
	
	public static int byteArrayToInt(byte[] b) {
	    final ByteBuffer bb = ByteBuffer.wrap(b);
	    bb.order(ByteOrder.LITTLE_ENDIAN);
	    return bb.getInt();
	}
	
	public int convert4(int ii, byte[] bb) {
	      char n41 = (char)bb[ii];
	      n41 = (char)(n41 & 255);
	      char n31 = (char)bb[ii + 1];
	      n31 = (char)(n31 & 255);
	      char n21 = (char)bb[ii + 2];
	      n21 = (char)(n21 & 255);
	      char n11 = (char)bb[ii + 3];
	      n11 = (char)(n11 & 255);
	      int valor = n11 << 8 | n21;
	      valor = valor << 8 | n31;
	      valor = valor << 8 | n41;
	      return valor;
	   }
	
	
	private String readLine(DataInputStream dis, int length) throws IOException{
		    byte[] bytes  = new byte[length + 8];
		    dis.read(bytes);
	        int end = length + 4;
	        int start = 4;
	        String data = new String (bytes).trim();
	       // System.out.println(data);
	        if(data.length()<(length + 4))
	        	return data;
	        else
	        	 return data.substring(start,end);
	}
	
	
	
	public static void main(String[] args) throws IOException{
		
		

		
//		
//		
		String file = "C:\\Users\\ledisi\\Documents\\projects\\odesk\\mentrics\\test_files\\2011-12-29-0420-00seisan test file.MAN___010.seisan";

//		String file = "C:\\Users\\ledisi\\Documents\\projects\\odesk\\mentrics\\test_files\\2008-11-21-0242-41S.PP____006";
		SeisanFile s = new SeisanFile();
//		try {
//			s.read(file);
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
		
		
		String ouputfile = "C:\\Users\\ledisi\\Documents\\projects\\odesk\\mentrics\\test_files\\daa.txt";
		
		FileInputStream fis = new FileInputStream(file);
		BufferedInputStream buf = new BufferedInputStream(fis);
		DataInputStream dis = new DataInputStream(buf);
		
		DataOutputStream dos = new DataOutputStream(new FileOutputStream(ouputfile));
		PrintWriter pw = new PrintWriter(dos);
		String data  = s.readLine(dis,80);
		pw.write(data);
		pw.println();
		pw.println();
	
		int channels = Integer.parseInt(data.substring(30,33).trim());
		
//		System.out.println("number of channels : "+ channels);
//		System.out.println();
//	    dis.skipBytes((5+channels)*88);
	    
	    int number_of_lines = (channels/ 3) + (channels % 3 + 1);
	    
	    if (number_of_lines < 10){
	        number_of_lines = 10;
	    }
	    dis.skipBytes(88);
	    dis.skipBytes((number_of_lines)*88);
	    
	    for(int i = 0 ; i < channels; i++){
	    	String data2  = s.readLine(dis,1040);
	    	pw.write(data2);
			pw.println();
			pw.println();
//	    	readChannel(dis);
	    }
	    dis.close();
	    pw.close();
	    
	    dos.close();
	}


	public ArrayList<Channel> getChannels() {
		return channels;
	}


	
	public Wave toWave()
	{
		Wave sw = new Wave();
		int size = 0;
		Float sampleRate = channels.get(0).getSampleRate();
		
		for(Channel c : channels){
			if(sampleRate >= c.getSampleRate()){
				sampleRate = c.getSampleRate();
			}
			size += c.getData().length;
		}
//		sw.setStartTime(startTime);
		sw.setSamplingRate(sampleRate);
		sw.buffer = new int[size];
		int index = 0;
		for (int i = 0; i < channels.size(); i++)
		{
			Channel c = channels.get(i);
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
