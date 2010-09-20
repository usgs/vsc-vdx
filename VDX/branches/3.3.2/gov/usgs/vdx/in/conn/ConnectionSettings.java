package gov.usgs.vdx.in.conn;

public class ConnectionSettings {
	
	public String deviceType;
	public String deviceIP;
	public int devicePort;
	public int callNumber;
	public int repeater;
	public int dataLines;
	public int connTimeout;
	public int dataTimeout;
	public int maxRetries;
	public String timeSource;
	public String instrument;
	public String delimiter;
	public String firmwareVersion;
	public int tiltid;
	
	public ConnectionSettings (String deviceType, String deviceIP, int devicePort, int callNumber, int repeater, int dataLines, 
			int connTimeout, int dataTimeout, int maxRetries, String timeSource, String delimiter, String firmwareVersion, int tiltid) {
		this.deviceType		= deviceType;
		this.deviceIP		= deviceIP;
		this.devicePort		= devicePort;
		this.callNumber		= callNumber;
		this.repeater		= repeater;
		this.dataLines		= dataLines;
		this.connTimeout	= connTimeout;
		this.dataTimeout	= dataTimeout;
		this.maxRetries		= maxRetries;
		this.timeSource		= timeSource;
		this.delimiter		= delimiter;
		this.tiltid			= tiltid;
	}
	
	public String toString() {
		return deviceType + "|" + deviceIP + "|" + devicePort + "|" + callNumber + "|" + repeater + "|" + dataLines + "|" + 
				connTimeout + "|" + dataTimeout + "|" + maxRetries + "|" + timeSource + "|" + delimiter + "|" + "|" + firmwareVersion + "|" + tiltid;
	}
	
	public String headerString() {
		return "deviceType|deviceIP|devicePort|callNumber|repeater|dataLines|connTimeout|dataTimeout|maxRetries|timeSource|delimiter|firmwareVersion|tiltid";
	}
}
