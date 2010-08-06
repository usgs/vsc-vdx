package gov.usgs.vdx.in.conn;

/**
 * Interface to represent connection to a device
 * 
 * @author Loren Antolik
 */
public interface Connection {
	
	public void connect () throws Exception;
	public void disconnect ();
	public void writeString (String dataRequest);
	public String readString (long dataTimeout);
	public void setEcho (boolean echo);
}