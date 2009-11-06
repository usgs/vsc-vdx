package gov.usgs.vdx.data;

import gov.usgs.vdx.db.VDXDatabase;

/**
 * Keeps all information needed to construct particular data source
 *
 * @author Dan Cervelli, Loren Antolik
 */
public class DataSourceDescriptor {
	
	private VDXDatabase database;
	
	private String source, className, description, vdxName;
	
	private String driver, url, vdxPrefix;
	
	private String dbName;
	
	private DataSource dataSource;
	
	/**
	 * Constructor
	 * @param nm data source name
	 * @param cn data source class name
	 * @param de data source description
	 * @param vn data source vdx name
	 * @param dv data source driver
	 * @param ul data source url
	 * @param pf data source prefix
	 */
	public DataSourceDescriptor(String nm, String cn, String de, String vn, String dv, String ul, String pf) {
		source		= nm;
		className	= cn;
		description	= de;
		vdxName		= vn;
		driver		= dv;
		url			= ul;
		vdxPrefix	= pf;
	}
	
	/**
	 * Getter for data source name
	 * @return
	 */
	public String getSource() {
		return source;
	}
	
	/**
	 * Getter for data source class name
	 */
	public String getClassName() {
		return className;
	}
	
	/**
	 * Getter for data source description
	 */
	public String getDescription() {
		return description;
	}

	private void instantiateDataSource() {
		try {
			database	= new VDXDatabase(driver, url, vdxPrefix);
			dataSource	= (DataSource)Class.forName(className).newInstance();
			dbName		= vdxName;
			Class.forName(className).getMethod("initialize", new Class[] { VDXDatabase.class, String.class }).invoke(dataSource, new Object[] { database, dbName });
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Construct data source using internal information if it isn't exist
	 * @return reference to constructed data source
	 */
	public DataSource getDataSource() {
		if (dataSource == null && className != null) {
			instantiateDataSource();
		}
		return dataSource;
	}
	
}
