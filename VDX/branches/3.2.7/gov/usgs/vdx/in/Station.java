package gov.usgs.vdx.in;

import java.util.List;

/**
 * Station data structure.  Used for storing all config file information related to a station.
 *
 * @author Loren Antolik
 */
public class Station {
	
	// station attributes
	public String station;
	public String stationType;
	public String name;
	public int tiltid;
	public double latitude;
	public double longitude;
	public double azimuthNom;
	
	// translations
	public double cx, dx, cy, dy, ch, dh, cb, db, ci, di, cg, dg, cr, dr, azimuthInst;
	public double cs1, ds1, cs2, ds2, cbar, dbar;
	public double cco2l, dco2l, cco2h, dco2h;
	
	// connection settings
	public int callNumber;
	public int repeater;
	public int connTimeout;
	public int dataTimeout;
	public int maxRetries;
	public String timeSource;
	public int syncInterval;
	public int dataLines;
	public String instrument;
	public String delimiter;
	
	// data types at station
	public List<DataType> dataTypeList;
	
	// based on station type
	public boolean hasTilt		= false;;
	public boolean hasStrain	= false;;
	public boolean hasGas		= false;
	
	/**
	 * default constructor
	 */
	public Station(){}

	/**
	 * Constructor.  Sets the variables and determines what type of station this is.
	 * @param station
	 * @param stationType
	 * @param name
	 * @param tiltid
	 * @param latitude
	 * @param longitude
	 * @param azimuthNom
	 * @param cx
	 * @param dx
	 * @param cy
	 * @param dy
	 * @param ch
	 * @param dh
	 * @param cb
	 * @param db
	 * @param ci
	 * @param di
	 * @param cg
	 * @param dg
	 * @param cr
	 * @param dr
	 * @param azimuthInst
	 * @param cs1
	 * @param ds1
	 * @param cs2
	 * @param ds2
	 * @param cbar
	 * @param dbar
	 * @param cco2l
	 * @param dco2l
	 * @param cco2h
	 * @param dco2h
	 * @param callNumber
	 * @param repeater
	 * @param connTimeout
	 * @param dataTimeout
	 * @param maxRetries
	 * @param timeSource
	 * @param syncInterval
	 * @param dataLines
	 * @param instrument
	 * @param delimiter
	 * @param dataTypeList
	 */
	public Station (String station, String stationType, String name, int tiltid, double latitude, double longitude, double azimuthNom,
			double cx, double dx, double cy, double dy, double ch, double dh, double cb, double db, double ci, double di,
			double cg, double dg, double cr, double dr, double azimuthInst, double cs1, double ds1, double cs2, double ds2,
			double cbar, double dbar, double cco2l, double dco2l, double cco2h, double dco2h, 
			int callNumber, int repeater, int connTimeout, int dataTimeout, int maxRetries, String timeSource, 
			int syncInterval, int dataLines, String instrument, String delimiter, List<DataType> dataTypeList) {
		
		this.station			= station;
		this.stationType		= stationType;
		this.name				= name;
		this.tiltid				= tiltid;
		this.latitude			= latitude;
		this.longitude			= longitude;
		this.azimuthNom			= azimuthNom;
		this.cx					= cx;
		this.dx					= dx;
		this.cy					= cy;
		this.dy					= dy;
		this.ch					= ch;
		this.dh					= dh;
		this.cb					= cb;
		this.db					= db;
		this.ci					= ci;
		this.di					= di;
		this.cg					= cg;
		this.dg					= dg;
		this.cr					= cr;
		this.dr					= dr;
		this.azimuthInst		= azimuthInst;
		this.cs1				= cs1;
		this.ds1				= ds1;
		this.cs2				= cs2;
		this.ds2				= ds2;
		this.cbar				= cbar;
		this.dbar				= dbar;
		this.cco2l				= cco2l;
		this.dco2l				= dco2l;
		this.cco2h				= cco2h;
		this.dco2h				= dco2h;
		this.callNumber			= callNumber;
		this.repeater			= repeater;
		this.connTimeout		= connTimeout;
		this.dataTimeout		= dataTimeout;
		this.maxRetries			= maxRetries;
		this.timeSource			= timeSource;
		this.syncInterval		= syncInterval;
		this.dataLines			= dataLines;
		this.instrument			= instrument;
		this.delimiter			= delimiter;
		this.dataTypeList		= dataTypeList;
		
		if (stationType.indexOf("tilt") >= 0) {
			hasTilt = true;
		}
		
		if (stationType.indexOf("strain") >= 0) {
			hasStrain = true;
		}
		
		if (stationType.indexOf("gas") >= 0) {
			hasGas = true;
		}
	}
}