package gov.usgs.vdx.data.wave;

import java.util.Date;

public class Channel {
    public static class SimpleChannel {
        public final String networkName;
        public final String stationCode;
        public final String firstTwoComponentCode;
        public final String lastComponentCode;
        public final String toString;


        public SimpleChannel(String toString, String networkName, String stationCode, String firstTwoComponentCode, String lastComponentCode) {
            this.networkName = networkName;
            this.stationCode = stationCode;
            this.firstTwoComponentCode = firstTwoComponentCode;
            this.lastComponentCode = lastComponentCode;
            this.toString = toString != null ? toString : generateString();
        }

        public String toString() {
            return toString;
        }

        // TODO: should do generateString for display only. Should see how SCNL does parsing and follow that convention
        // for toString and parse
        public String generateString() {
            return (((networkName == null || networkName.trim().length() == 0) ? "--"
                    : networkName)
                    + "  "
                    + ((stationCode == null || stationCode.trim().length() == 0) ? "--"
                    : stationCode)
                    + "  "
                    + ((firstTwoComponentCode == null || firstTwoComponentCode.trim().length() == 0) ? ((lastComponentCode == null || lastComponentCode
                    .trim().length() == 0) ? "--"
                    : lastComponentCode)
                    : ((lastComponentCode == null || lastComponentCode
                    .trim().length() == 0) ? firstTwoComponentCode
                    : firstTwoComponentCode + lastComponentCode)));
        }

        public static SimpleChannel parse(String channel) {
            try {
                String compressed = channel.replace("  ", " ");
                String[] split = compressed.split(" ");
                String network = "--".equals(split[0]) ? null : split[0];
                int len = split[2].length();
                String c1 = split[2].substring(0, len-1);
                String c2 = split[2].substring(len-1, len);
                return new SimpleChannel(channel, network, split[1], c1, c2);
            } catch (Exception e) {
                System.out.println("Could not parse channel: "+channel);
                return new SimpleChannel(channel, null, null, null, null);
            }
        }

        public void populateSAC(SAC sac) {
            sac.kstnm = stationCode;
            sac.kcmpnm = firstTwoComponentCode+lastComponentCode;
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
            return hasData(stationCode) && hasData(firstTwoComponentCode) && hasData(lastComponentCode);
        }

        // TODO: move to general place and use
        public static boolean hasData(String s) {
            return s != null && !s.isEmpty();
        }

        public String fullComponent() {
            return firstTwoComponentCode+lastComponentCode;
        }
    }

    public final SimpleChannel channel;
	private String firstLocationCode;
	private Integer year;
	private String secondLocationCode;
	private String doy;
	private String firstNetworkCode;
	private Integer month;
	private String secondNetworkCode;
	private Integer day;
	private Integer hour;
	private Integer minute;
	private String timingIndicator;
	private Float second;
	private Float sampleRate;
	private Integer numberOfSamples;
	private int[] data;
	
	private Date d = new Date();
	private double startTime; 
	
	
	public String toString(){
		return channel.stationCode + ", " + channel.firstTwoComponentCode + ", "+ firstLocationCode + ","+channel.lastComponentCode+","+sampleRate+","+numberOfSamples;
	}
	
	public void setDate(){
		System.out.println(year+" " + month);
		d.setYear(year);
		d.setMonth(month - 1);
		d.setHours(hour);
		d.setDate(day);
		d.setMinutes(minute);
		d.setSeconds(0);
		d.setTime(d.getTime()+(int)(second * 1000));
	}
	
	public Channel(String header){
        channel = new SimpleChannel(null,
                header.substring(16,17),
                header.substring(0,5).trim(),
                header.substring(5,7).trim(),
                header.substring(7,8).trim());
//		firstLocationCode = header.substring(7,8).trim();
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
		
		setDate();
		
		startTime = d.getTime();
		
	}


	public String getFirstLocationCode() {
		return firstLocationCode;
	}


	public void setFirstLocationCode(String firstLocationCode) {
		this.firstLocationCode = firstLocationCode;
	}


	public Integer getYear() {
		return year;
	}


	public void setYear(Integer year) {
		this.year = year;
	}


	public String getSecondLocationCode() {
		return secondLocationCode;
	}


	public void setSecondLocationCode(String secondLocationCode) {
		this.secondLocationCode = secondLocationCode;
	}


	public String getDoy() {
		return doy;
	}


	public void setDoy(String doy) {
		this.doy = doy;
	}


	public String getFirstNetworkCode() {
		return firstNetworkCode;
	}


	public void setFirstNetworkCode(String firstNetworkCode) {
		this.firstNetworkCode = firstNetworkCode;
	}


	public Integer getMonth() {
		return month;
	}


	public void setMonth(Integer month) {
		this.month = month;
	}


	public Integer getDay() {
		return day;
	}


	public void setDay(Integer day) {
		this.day = day;
	}


	public Integer getHour() {
		return hour;
	}


	public void setHour(Integer hour) {
		this.hour = hour;
	}


	public Integer getMinute() {
		return minute;
	}


	public void setMinute(Integer minute) {
		this.minute = minute;
	}


	public String getTimingIndicator() {
		return timingIndicator;
	}


	public void setTimingIndicator(String timingIndicator) {
		this.timingIndicator = timingIndicator;
	}


	public Float getSecond() {
		return second;
	}


	public void setSecond(Float second) {
		this.second = second;
	}


	public Float getSampleRate() {
		return sampleRate;
	}


	public void setSampleRate(Float sampleRate) {
		this.sampleRate = sampleRate;
	}


	public Integer getNumberOfSamples() {
		return numberOfSamples;
	}


	public void setNumberOfSamples(Integer numberOfSamples) {
		this.numberOfSamples = numberOfSamples;
	}

	public int[] getData() {
		return data;
	}

	public void setData(int[] data) {
		this.data = data;
	}
	
	
	
	
	
	public double getStartTime() {
		return startTime;
	}

	
	public Wave toWave()
	{
		Wave sw = new Wave();
		sw.setSamplingRate(getSampleRate());
//		sw.setStartTime(startTime);
		sw.buffer = getData();
		return sw;
	}

	
}
