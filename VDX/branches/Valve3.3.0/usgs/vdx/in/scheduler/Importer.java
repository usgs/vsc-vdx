package gov.usgs.vdx.in.scheduler;

/**
 * General interface to represent import task to be scheduled
 */
public interface Importer {

	/**
	 * Initialize instance
	 * @param args command line arguments
	 */
	public void init(String[] args);
	
}
