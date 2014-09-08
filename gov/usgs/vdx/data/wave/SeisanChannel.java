package gov.usgs.vdx.data.wave;

import gov.usgs.util.Util;
import gov.usgs.vdx.data.wave.SeisanChannel.ComponentDirection;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;



/**
 * This class holds meta data for a Seisan channel
 * 
 * @author Chirag Patel
 *
 */
public class SeisanChannel {
    
	public final SimpleChannel channel;
	private String firstLocationCode;
	private Integer year;
	private String secondLocationCode;
	private String doy;
	private Integer month;
	private Integer day;
	private Integer hour;
	private Integer minute;
	private Float second;
	private Float sampleRate;
	private Integer numberOfSamples;
	private int[] data;
	private Date startDate;
	
	/**
	 * parses and generates channel info from a String
	 * 
	 * @param header
	 */
	public SeisanChannel(String header) {
        channel = new SimpleChannel(null,
                header.substring(16,17)+header.substring(19,20),
                header.substring(0,5).trim(),
                header.substring(5,9).trim());

		year = header.substring(9,12).trim().length() == 0?null:Integer.parseInt(header.substring(9,12).trim());
		secondLocationCode = header.substring(12,13).trim();
		doy  = header.substring(13,16).trim();
		month = header.substring(17,19).trim().length() == 0?null:Integer.parseInt(header.substring(17,19).trim());
		day  = header.substring(20,22).trim().length() == 0?null:Integer.parseInt(header.substring(20,22).trim());
		hour  = header.substring(23,25).trim().length() == 0?null:Integer.parseInt(header.substring(23,25).trim());
		minute  = header.substring(26,28).trim().length() == 0?null:Integer.parseInt(header.substring(26,28).trim());
		second  = header.substring(29,35).trim().length() == 0?null:Float.parseFloat(header.substring(29,35).trim());

		sampleRate = header.substring(36,43).trim().length()==0?null:Float.parseFloat(header.substring(36,43).trim());
		numberOfSamples = header.substring(43,50).trim().length()==0?null:Integer.parseInt(header.substring(43,50).trim());
		
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
		
		System.out.println(c.getTime());
		System.out.println((year)+ " - " + month + " - " + day + "  " + hour + " : " + minute + ":" + ":"+second);
		
	}

	public Float getSampleRate() {
		return sampleRate;
	}



	public Integer getNumberOfSamples() {
		return numberOfSamples;
	}


	public int[] getData() {
		return data;
	}

	public void setData(int[] data) {
		this.data = data;
	}
	
	public Wave toWave()
	{
		Wave sw = new Wave();
		sw.setSamplingRate(sampleRate);
		sw.setStartTime(Util.dateToJ2K(startDate));
		sw.buffer = data;
		return sw;
	}
	
	
	public String toString(){
		return channel.stationCode + ", " + channel.firstTwoComponentCode + ", "+ firstLocationCode + ","+channel.lastComponentCode+","+sampleRate+","+numberOfSamples;
	}
	
	public static enum ComponentDirection {
		VERTICAL, NORTH, EAST;

		public static ComponentDirection fromString(String comp) {
			if ("Z".equals(comp)) {
				return VERTICAL;
			} else if ("N".equals(comp)) {
				return NORTH;
			} else if ("E".equals(comp)) {
				return EAST;
			} else {
				throw new IllegalArgumentException("Unknown component code: "+comp);
			}
		}
	}
	
	public static class SimpleChannel {
		public static SimpleChannel parse(String channel) {
			String[] values;
			if (channel.contains("_")) {
				values = channel.split("_");
			} else {
				values = channel.split(" ");
			}
			if (values.length < 2) {
				throw new UnsupportedOperationException(
						"Could not parse channel info: " + channel);
			}
			String station = values[0];
			String fullComponent = values[1];
			String network = values.length >= 3 ? values[2] : "";
			return new SimpleChannel(channel, network, station, fullComponent);
		}

		public final String networkName;
        public final String stationCode;
        public final String fullComponent;
        public final String firstTwoComponentCode;
        public final String lastComponentCode;
        public final String toString;


        public SimpleChannel(String toString, String networkName, String stationCode, String fullComponent) {
            this.networkName = networkName;
            this.stationCode = stationCode;
            this.fullComponent = fullComponent;
			int compLen = fullComponent.length();
			this.firstTwoComponentCode = compLen > 1 ? fullComponent.substring(0, compLen - 1) : "";
			this.lastComponentCode = compLen > 0 ? fullComponent.substring(compLen-1, compLen) : "";
            this.toString = toString != null ? toString : generateString();
        }

        public String toString() {
            return toString;
        }

        public String generateString() {
    		return trim(stationCode) + "_" + trim(fullComponent) + "_" + trim(networkName);
        }
        
        public static String trim(String in) {
        	return in != null ? in.trim() : "";
        }

        public void populateSAC(SAC sac) {
            sac.kstnm = stationCode;
            sac.kcmpnm = fullComponent;
            sac.knetwk = networkName;
        }

        public String showStationCode() {
            return stationCode == null ? "" : stationCode;
        }

        public String showNetworkName() {
            return networkName == null ? "" : networkName;
        }

        public String showFirstTwoComponent() {
            return firstTwoComponentCode == null ? "" : firstTwoComponentCode;
        }

        public boolean isPopulated() {
            return hasData(stationCode) && hasData(fullComponent);
        }

        // TODO: move to general place and use
        public static boolean hasData(String s) {
            return s != null && !s.isEmpty();
        }

        public String fullComponent() {
            return fullComponent;
        }
        
        public String getLastComponentCode(){
        	return lastComponentCode;
        }

		public boolean isDirection(ComponentDirection direction) {
			return direction == ComponentDirection.fromString(getLastComponentCode());
		}

		public ComponentDirection getDirection() {
			return ComponentDirection.fromString(getLastComponentCode());
		}
    }
}
