package gov.usgs.vdx.data.heli.plot;

import gov.usgs.plot.Plot;
import gov.usgs.vdx.data.heli.HelicorderData;

import java.awt.Color;
import java.util.TimeZone;

/**
 * A class that encapsulated the settings for a HelicorderRenderer.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.3  2005/09/22 20:53:24  dcervelli
 * Added largeChannelDisplay.
 *
 * Revision 1.2  2005/08/29 19:34:49  tparker
 * Convert barMult to public float for auto-scale slider mod
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * Revision 1.3  2005/05/01 16:57:14  cervelli
 * Changes for minimal axes.
 *
 * Revision 1.2  2005/04/27 22:29:16  cervelli
 * Specified background color.
 *
 * Revision 1.1  2005/04/10 16:23:58  cervelli
 * Initial revision.
 *
 * @author Dan Cervelli
 */
public class HelicorderSettings
{
	public String channel;
	public double timeChunk = 20 * 60;
	public double endTime;
	public double startTime;

	public int width = 1000;
	public int height = 1000;

	public int left = 70;
	public int top = 20;

	public int clipValue = -1;
	public int barRange = -1;
	public float barMult = 3;
	public boolean showClip = false;
	public boolean forceCenter = false;
	
	public String timeZoneAbbr = "GMT";
	public double timeZoneOffset = 0;
	public TimeZone timeZone = TimeZone.getTimeZone("GMT");
	
	public boolean minimumAxis = false;
	public boolean largeChannelDisplay = false;
	
	/**
	 * Apply settings stored in this class to HelicorderRenderer
	 * @param hr renderer to tune
	 * @param hd data to apply to renderer
	 */
	public void applySettings(HelicorderRenderer hr, HelicorderData hd)
	{
		hr.setChannel(channel);
		hr.setData(hd);
		hr.setTimeChunk(timeChunk);
		hr.setLocation(left, top, width, height);
		hr.setForceCenter(forceCenter);
		double mean = hd.getMeanMax();
		double bias = hd.getBias();
		mean = Math.abs(bias - mean);

		// auto-scale
		if (minimumAxis)
			barMult = 6;
		if (clipValue == -1)
			clipValue = (int)(21 * mean);
		if (barRange == -1)
			barRange = (int)(barMult * mean);
		
		hr.setHelicorderExtents(startTime, endTime, -1 * Math.abs(barRange), Math.abs(barRange));
		hr.setClipValue(clipValue);
		hr.setShowClip(showClip);
		hr.setTimeZone(timeZone);
//		hr.setTimeZoneAbbr(timeZoneAbbr);
//		hr.setTimeZoneOffset(timeZoneOffset);
		hr.setLargeChannelDisplay(largeChannelDisplay);
		if (minimumAxis)
			hr.createMinimumAxis();
		else
			hr.createDefaultAxis();
	}
	
	/**
	 * Compute helicorder size
	 * @param w plot width
	 * @param h plot height
	 */
	public void setSizeFromPlotSize(int w, int h)
	{
		width = w - left * 2;
		height = h - (top + 50);
	}
	
	/**
	 * set helicorder position
	 */
	public void setMinimumSizes()
	{
		left = 31;
		top = 16;
	}
	
	/**
	 * Create plot 
	 * @param hd data to render
	 */
	public Plot createPlot(HelicorderData hd)
	{
		Plot plot = new Plot();
		plot.setSize(width + 140, height + 70);
		plot.setBackgroundColor(new Color(0.97f, 0.97f, 0.97f));
		HelicorderRenderer hr = new HelicorderRenderer();
		hr.setChannel(channel);
		applySettings(hr, hd);
		plot.addRenderer(hr);
		return plot;
	}
}