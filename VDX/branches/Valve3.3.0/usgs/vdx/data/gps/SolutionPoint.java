package gov.usgs.vdx.data.gps;

public class SolutionPoint {
	public String channel;
	public DataPoint dp;
	
	public SolutionPoint() {
		dp = new DataPoint();
	}
}