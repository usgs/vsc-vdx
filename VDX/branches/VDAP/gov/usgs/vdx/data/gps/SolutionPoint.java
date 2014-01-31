package gov.usgs.vdx.data.gps;

/**
 * Point in gps solution
 */
public class SolutionPoint {
	public String channel;
	public DataPoint dp;
	
	/**
	 * Default constructor
	 */
	public SolutionPoint() {
		dp = new DataPoint();
	}
}