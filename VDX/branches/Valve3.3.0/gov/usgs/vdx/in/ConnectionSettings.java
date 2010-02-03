package gov.usgs.vdx.in;

public class ConnectionSettings {
	
	public int callNumber;
	public int repeater;
	public int dataLines;
	public int connTimeout;
	public int dataTimeout;
	public int maxRetries;
	public String timeSource;
	public String instrument;
	public String delimiter;
	public int tiltid;
	
	public ConnectionSettings (int callNumber, int repeater, int dataLines, int connTimeout, int dataTimeout,
			int maxRetries, String timeSource, String instrument, String delimiter, int tiltid) {
		this.callNumber		= callNumber;
		this.repeater		= repeater;
		this.dataLines		= dataLines;
		this.connTimeout	= connTimeout;
		this.dataTimeout	= dataTimeout;
		this.maxRetries		= maxRetries;
		this.timeSource		= timeSource;
		this.instrument		= instrument;
		this.delimiter		= delimiter;
		this.tiltid			= tiltid;
	}
	
	public String toString() {
		return callNumber + "|" + repeater + "|" + dataLines + "|" + connTimeout + "|" + dataTimeout + "|" + 
		       maxRetries + "|" + timeSource + "|" + instrument + "|" + delimiter + "|" + tiltid;
	}
	
	public String headerString() {
		return "callNumber|repeater|dataLines|connTimeout|dataTimeout|maxRetries|timeSource|instrument|delimiter|tiltid";
	}
}
