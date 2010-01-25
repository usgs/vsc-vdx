package gov.usgs.vdx.in;

public class ConnectionSettings {
	
	public int callNumber;
	public int repeater;
	public int dataLines;
	public int connTimeout;
	public int dataTimeout;
	public int maxRetries;
	public String timeSource;
	public int syncInterval;
	public String instrument;
	public String delimiter;
	public int tiltid;
	
	public ConnectionSettings (int callNumber, int repeater, int dataLines, int connTimeout, int dataTimeout,
			int maxRetries, String timeSource, int syncInterval, String instrument, String delimiter, int tiltid) {
		this.callNumber		= callNumber;
		this.repeater		= repeater;
		this.dataLines		= dataLines;
		this.connTimeout	= connTimeout;
		this.dataTimeout	= dataTimeout;
		this.maxRetries		= maxRetries;
		this.timeSource		= timeSource;
		this.syncInterval	= syncInterval;
		this.instrument		= instrument;
		this.delimiter		= delimiter;
		this.tiltid			= tiltid;
	}
}
