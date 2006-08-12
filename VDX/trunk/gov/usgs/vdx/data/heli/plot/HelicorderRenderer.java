package gov.usgs.vdx.data.heli.plot;

import gov.usgs.plot.AxisRenderer;
import gov.usgs.plot.FrameDecorator;
import gov.usgs.plot.FrameRenderer;
import gov.usgs.plot.SmartTick;
import gov.usgs.plot.TextRenderer;
import gov.usgs.util.Time;
import gov.usgs.util.Util;
import gov.usgs.vdx.data.heli.HelicorderData;

import java.awt.Color;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.AffineTransform;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.DataLine;
import javax.sound.sampled.SourceDataLine;

import cern.colt.matrix.DoubleMatrix2D;

/**
 * A class for rendering helicorders.
 * 
 * $Log: not supported by cvs2svn $
 * Revision 1.10  2006/07/22 20:15:02  cervelli
 * Changes for time zones.
 *
 * Revision 1.9  2006/04/08 01:50:03  dcervelli
 * Added getRowHeight() function for bug #17.
 *
 * Revision 1.8  2006/02/01 23:26:11  tparker
 * Play clip alert in a dedicated thread
 *
 * Revision 1.7  2006/02/01 18:01:31  tparker
 * tweak alert
 *
 * Revision 1.6  2006/01/25 21:46:26  tparker
 * Move clipping alert into the heli renderer.
 *
 * Revision 1.5  2005/09/22 20:54:00  dcervelli
 * Moved large channel display code from Swarm to here.
 *
 * Revision 1.4  2005/09/14 20:41:17  dcervelli
 * Fixed minimum axis channel label position bug (Mantis #0000001).
 *
 * Revision 1.3  2005/08/29 22:35:07  dcervelli
 * Fixed first line color change bug.
 *
 * Revision 1.2  2005/08/29 22:15:48  dcervelli
 * Optimized forceCenter.
 *
 * Revision 1.1  2005/08/26 20:39:00  dcervelli
 * Initial avosouth commit.
 *
 * Revision 1.6  2005/05/01 16:57:14  cervelli
 * Changes for minimal axes.
 *
 * Revision 1.5  2005/04/23 15:52:51  cervelli
 * Removed code timer, cleaned up.
 *
 * Revision 1.4  2005/04/17 16:34:52  cervelli
 * Optimizations.
 *
 * Revision 1.3  2004/10/13 02:52:11  cvs
 * Removed output of clipped value.
 *
 * Revision 1.2  2004/10/13 02:48:46  cvs
 * Fixed bug where NO_DATA samples were plotting.
 *
 * @author Dan Cervelli
 */
public class HelicorderRenderer extends FrameRenderer
{
	private static final Font LARGE_FONT = Font.decode("Dialog-BOLD-48");
	private HelicorderData data;
	
	private boolean forceCenter;
	private double timeChunk;
	private int numRows;
	private double rowHeight;
	private double hcMinX;
	private double hcMaxX;
	private double hcMinY;
	private double hcMaxY;
	private Color[] colors = 
		new Color[] {new Color(0, 0, 255), new Color(0, 0, 205), new Color(0, 0, 155), new Color(0, 0, 105)};
		
	private TimeZone timeZone = TimeZone.getTimeZone("UTC");
	
	private int clipValue = 3000;
	private boolean showClip = false;
	private boolean alertClip = false;
	private String clipWav;
	private int alertClipTimeout;
	private double lastClipAlert;
	private double lastClipTime;
	
	private String channel;
	private boolean largeChannelDisplay;
	
	private FrameDecorator decorator;
	
	public HelicorderRenderer()
	{
		forceCenter = false;
	}		
		
	public HelicorderRenderer(HelicorderData d, double xs)
	{
		this();
		data = d;
		timeChunk = xs;
	}
	
	public void setData(HelicorderData d)
	{
		data = d;	
	}
	
	public void setTimeChunk(double xs)
	{
		timeChunk = xs;	
	}
	
	public double getTimeChunk()
	{
		return timeChunk;	
	}
	
	public void setLargeChannelDisplay(boolean b)
	{
		largeChannelDisplay = b;
	}
	
	public double[] getTranslationInfo(boolean adjTime)
	{
		double tzo = 0;
		if (adjTime)
			tzo = Time.getTimeZoneOffset(timeZone, getViewEndTime());
		return new double[] {graphX, graphX + graphWidth, rowHeight, graphY, hcMinX + tzo, hcMaxX + tzo, timeChunk, timeChunk / graphWidth};
	}
	
	/** Gets the x-scale (the graph width / horizontal view extent).
	 * @return the x-scale
	 */
	public double helicorderGetXScale()
	{
		return graphWidth / timeChunk;
	}
	
	/** Gets the y-scale (the graph height / vertical view extent).
	 * @return the y-scale
	 */
	public double helicorderGetYScale()
	{
		return ((graphHeight / (double)numRows) / (hcMaxY - hcMinY));
	}
	
	public double helicorderGetXPixel(double x)
	{
		double tx = (x - hcMinX) % timeChunk;
		return (tx * helicorderGetXScale()) + graphX;
	}
	
	public double helicorderGetYPixel(double x, double y)
	{
		int row = numRows - (int)((x - hcMinX) / timeChunk) - 1;
		return graphY + graphHeight - ((y - hcMinY) * helicorderGetYScale()) - ((double)row * rowHeight);
	}
	
	public double getHelicorderMaxX()
	{
		return hcMaxX;
	}
	
	public double getHelicorderMinX()
	{
		return hcMinX;
	}
	
	public double getViewEndTime()
	{
		return hcMinX + numRows * timeChunk;
	}
	
	/** Sets the view extents of the frame.
	 * @param loX the minimum x view extent
	 * @param hiX the maximum x view extent
	 * @param loY the minimum y view extent
	 * @param hiY the maximum y view extent
	 */
	public void setHelicorderExtents(double loX, double hiX, double loY, double hiY)
	{
		hcMinY = loY;
		hcMaxY = hiY;		
		hcMinX = loX - (loX % timeChunk);
		hcMaxX = hiX + (timeChunk - (hiX % timeChunk));
		numRows = (int)((hcMaxX - hcMinX) / timeChunk);
		rowHeight = (double)graphHeight / (double)numRows;
		
		super.setExtents(0, timeChunk, 0, numRows);
	}
	
	public int getRow(double x)
	{
		if (x - hcMinX < 0)
			return -1;
		return (int)((x - hcMinX) / (double)timeChunk);
	}
	
	public void setChannel(String ch)
	{
		channel = ch.replace('$', ' ');
	}
	
	/**
	 * @param clipBars The clipBars to set.
	 */
	public void setClipBars(int clipBars)
	{
	}
	
	/**
	 * @param cw The .wav to play when clipping is detected
	 */
	public void setClipWav(String cw)
	{
		clipWav = cw;
	}
	
	public void setClipAlertTimeout(int to)
	{
		alertClipTimeout = to;
	}
	/**
	 * @param forceCenter The forceCenter to set.
	 */
	public void setForceCenter(boolean forceCenter)
	{
		this.forceCenter = forceCenter;
	}
	
	public int getNumRows()
	{
		return numRows;	
	}

	public void setTimeZone(TimeZone tz)
	{
		timeZone = tz;
	}
	
	public void setTimeZoneAbbr(String s)
	{
//		timeZoneAbbr = s;
	}
	
	public void setTimeZoneOffset(double h)
	{
//		timeZoneOffset = h;
	}
	
	public void setShowClip(boolean b)
	{
		showClip = b;
	}
	
	public void setAlertClip(boolean b)
	{
		alertClip = b;
	}
	
	public void setClipValue(int i)
	{
		clipValue = i;
	}
	
	public double getRowHeight()
	{
		return rowHeight;
	}
	
	public void setFrameDecorator(FrameDecorator d)
	{
		decorator = d;
	}
	
	public void render(Graphics2D g)
	{
//		CodeTimer ct = new CodeTimer("render");
		if (data == null)
			return;
		
		if (decorator != null)
			decorator.decorate(this);
		
		AffineTransform origAT = g.getTransform();
		Color origColor = g.getColor();
		Shape origClip = g.getClip();
		
		if (axis != null)
			axis.render(g);

		g.setClip(new Rectangle(graphX + 1, graphY + 1, graphWidth - 1, graphHeight - 1));
		
		DoubleMatrix2D j2k = data.getTimes();
		DoubleMatrix2D min = data.getMin();
		DoubleMatrix2D max = data.getMax();
		
		double t1, x, y, w, h, ymax, ymin;
		double t2 = 0;
		
		double bias = Double.NaN;
		int lastRow = -1;
		int numRows = j2k.rows();
		Color lastColor = null;
		for (int j = 0; j < numRows; j++)
		{
			t1 = j2k.getQuick(j, 0);
			int k = ((int)((t1 - hcMinX) / (double)timeChunk)) % colors.length;
			if (k < 0)
				k = 0;
			if (lastColor != colors[k])
			{
				g.setColor(colors[k]);
				lastColor = colors[k];
			}
			
			t2 = t1 + 1;
			
			int r = getRow(t2);
			if (r != lastRow)
			{
				double st = hcMinX + r * timeChunk;
				bias = data.getBiasBetween(st, st + timeChunk);
				lastRow = r;
			}
			x = helicorderGetXPixel(t1);
			w = helicorderGetXPixel(t2) - x;
			ymax = max.getQuick(j, 0);
			ymin = min.getQuick(j, 0);
			
			if (ymax == Integer.MIN_VALUE || ymin == Integer.MIN_VALUE)
				continue;
			
			ymax -= bias;
			ymin -= bias;
			
			if (showClip && (ymax >= clipValue || ymin <= -clipValue))
			{
				lastClipTime = t1;
				if (lastColor != Color.red)
				{
					g.setColor(Color.red);
					lastColor = Color.red;
				}
			}
			
			if (ymax > clipValue)
				ymax = clipValue;
			
			if (ymin < -clipValue)
				ymin = -clipValue;
			
			y = helicorderGetYPixel(t1, ymax);
			h = helicorderGetYPixel(t1, ymin) - y;
			int hgt = (int)(h + 1);
			if (hgt < 1)
				hgt = 1;
			if (forceCenter)
				y = helicorderGetYPixel(t1, 0) - hgt / 2;
			g.fillRect((int)(x + 1), (int)(y + 1), (int)(w + 1), hgt);
		}
		
		g.setClip(origClip);
		g.setColor(origColor);
		g.setTransform(origAT);
		
		if (axis != null)
			axis.postRender(g);
		
		if (largeChannelDisplay && channel != null)
		{
			Font oldFont = g.getFont();
			g.setFont(LARGE_FONT);
			String c = channel.replace('_', ' ');
			FontMetrics fm = g.getFontMetrics();
			int width = fm.stringWidth(c);
			int height = fm.getAscent() + fm.getDescent();
			int lw = width + 20;
			if (alertClip && lastClipTime > t2 - alertClipTimeout && clipWav != null)
			{
				g.setColor(Color.red);
				if ((lastClipTime > lastClipAlert + alertClipTimeout))
				{
					lastClipAlert = t2;
					playClipAlert();
				}
			}
			else
				g.setColor(Color.white);
				
			g.fillRect(graphX + graphWidth / 2 - lw / 2, 3, lw, height);
			g.setColor(Color.black);
			g.drawRect(graphX + graphWidth / 2 - lw / 2, 3, lw, height);
			
			g.drawString(c, graphX + graphWidth / 2 - width / 2, height - fm.getDescent());
			g.setFont(oldFont);
		}
//		ct.stop();
	}
	
	public void createMinimumAxis()
	{
		decorator = new MinimumDecorator();
	}
	
	class MinimumDecorator extends FrameDecorator
	{
		public void decorate(FrameRenderer fr)
		{
			axis = new AxisRenderer(fr);
			axis.createDefault();
			
			int minutes = (int)Math.round(timeChunk / 60.0);
			int majorTicks = minutes;
			if (minutes > 30 && minutes < 180)
				majorTicks = minutes / 5;
			else if (minutes >= 180 && minutes < 360)
				majorTicks = minutes / 10;
			else if (minutes >= 360)
				majorTicks = minutes / 20;
			double[] mjt = SmartTick.intervalTick(minX, maxX, majorTicks);
			
			axis.createBottomTicks(null, mjt);
			axis.createTopTicks(null, mjt);
			axis.createVerticalGridLines(mjt);
			
			String[] btl = new String[mjt.length];
			for (int i = 0; i < mjt.length; i++)
				btl[i] = Long.toString(Math.round(mjt[i] / 60.0));
				
	 		double[] labelPosLR = new double[numRows];
	 		String[] leftLabelText = new String[numRows];
			DateFormat timeFormat = new SimpleDateFormat("HH:mm");
			DateFormat dayFormat = new SimpleDateFormat("yyyy-MM-dd");
			timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			dayFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			double pixelsPast = 0;
			double pixelsPerRow = graphHeight / numRows;
	 		for (int i = numRows - 1; i >= 0; i--)
	 		{
	 			pixelsPast += pixelsPerRow;
	 			labelPosLR[i] = i + 0.5;
	 			// TODO: fix
//	 			java.util.Date dtz = Util.j2KToDate(hcMaxX - (i + 1) * timeChunk + timeZoneOffset * 3600);
	 			java.util.Date dtz = Util.j2KToDate(hcMaxX - (i + 1) * timeChunk);
		 		String ftl = timeFormat.format(dtz);
		 		
				leftLabelText[i] = null;
	 			if (pixelsPast > 20)
	 			{
	 				leftLabelText[i] = ftl;
		 			pixelsPast = 0;
		 		}
	 		}

			axis.createLeftTickLabels(labelPosLR, leftLabelText);
			axis.addRenderer(new TextRenderer(graphX, graphY - 3, channel + ", " + dayFormat.format(Util.j2KToDate(hcMaxX))));
			
			double[] hg = new double[numRows - 1];
			for(int i = 0; i < numRows - 1; i++)
				hg[i] = i + 1.0;
				
			axis.createHorizontalGridLines(hg); 

			axis.setBackgroundColor(Color.white);
		}
	}
	
	class StandardDecorator extends FrameDecorator
	{
		public void decorate(FrameRenderer fr)
		{
			axis = new AxisRenderer(fr);
			axis.createDefault();
//			if (numRows <= 0)
//				return;
			
			int minutes = (int)Math.round(timeChunk / 60.0);
			int majorTicks = minutes;
			if (minutes > 30 && minutes < 180)
				majorTicks = minutes / 5;
			else if (minutes >= 180 && minutes < 360)
				majorTicks = minutes / 10;
			else if (minutes >= 360)
				majorTicks = minutes / 20;
			double[] mjt = SmartTick.intervalTick(minX, maxX, majorTicks);
			
			int minorTicks = 0;
			if (minutes <= 30)
				minorTicks = (int)Math.round(timeChunk / 10.0);
			else if (minutes > 30 && minutes <= 180)
				minorTicks = minutes;
			else if (minutes > 180)
				minorTicks = minutes / 5;
			double[] mnt = SmartTick.intervalTick(minX, maxX, minorTicks);
			
			axis.createBottomTicks(mjt, mnt);
			axis.createTopTicks(mjt, mnt);
			axis.createVerticalGridLines(mjt);
			
			String[] btl = new String[mjt.length];
			for (int i = 0; i < mjt.length; i++)
				btl[i] = Long.toString(Math.round(mjt[i] / 60.0));
				
			axis.createBottomTickLabels(mjt, btl);
			axis.setBottomLabelAsText("+ Minutes");
			
	 		double[] labelPosLR = new double[numRows];
	 		String[] leftLabelText = new String[numRows];
			String[] rightLabelText = new String[numRows]; 		
			DateFormat timeFormat = new SimpleDateFormat("HH:mm");
			DateFormat dayFormat = new SimpleDateFormat("MM-dd");
			timeFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			dayFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			
			boolean dst = timeZone.inDaylightTime(Util.j2KToDate(getViewEndTime()));
			double timeOffset = Time.getTimeZoneOffset(timeZone, dst);
			
			double pixelsPast = 0;
			double pixelsPerRow = graphHeight / numRows;
			String lastDayL = "";
			String lastDayR = "";
	 		for (int i = numRows - 1; i >= 0; i--)
	 		{
	 			pixelsPast += pixelsPerRow;
	 			labelPosLR[i] = i + 0.5;
	 			double j2ks = hcMaxX - (i + 1) * timeChunk;
	 			double j2ke = j2ks + timeChunk;
	 			java.util.Date dtz = Util.j2KToDate(j2ks + timeOffset);
		 		String ftl = timeFormat.format(dtz);
		 		String fdl = dayFormat.format(dtz);
		 		
		 		java.util.Date dutc = Util.j2KToDate(j2ke);
		 		String ftr = timeFormat.format(dutc);
		 		String fdr = dayFormat.format(dutc);

				leftLabelText[i] = null;
				if (!fdl.equals(lastDayL))
	 				leftLabelText[i] = fdl + "           ";
		 			
		 		if (timeOffset != 0 && !fdr.equals(lastDayR))
		 			rightLabelText[i] = "           " + fdr;
		 			
		 		lastDayL = fdl;
		 		lastDayR = fdr;
		 		
	 			if (pixelsPast > 20)
	 			{
	 				if (leftLabelText[i] != null)
		 				leftLabelText[i] = fdl + " " + ftl;
		 			else
		 				leftLabelText[i] = ftl;
		 			
		 			if (timeOffset != 0)
		 			{
			 			if (rightLabelText[i] != null)
			 				rightLabelText[i] = ftr + " " + fdr;
			 			else
			 				rightLabelText[i] = ftr;
			 		}
		 			pixelsPast = 0;
		 		}
	 		}

			axis.createLeftTickLabels(labelPosLR, leftLabelText);
			axis.createRightTickLabels(labelPosLR, rightLabelText);
			axis.setBottomLeftLabelAsText("Time (" + timeZone.getDisplayName(dst, TimeZone.SHORT) + ")");
			if (timeOffset != 0)
				axis.setBottomRightLabelAsText("Time (UTC)");
			
			double[] hg = new double[numRows - 1];
			for(int i = 0; i < numRows - 1; i++)
				hg[i] = i + 1.0;
				
			axis.createHorizontalGridLines(hg); 
			axis.setBackgroundColor(Color.white);
		}
	}
	
	/** Creates the default axis using SmartTick.
	 * @param hTicks suggested number of horizontal ticks
	 * @param vTicks suggested number of vertical ticks
	 * @param hExpand should horizontal be expanded for better ticks
	 * @param vExpand should vertical be expanded for better ticks
	 */
	public void createDefaultAxis()
	{
		decorator = new StandardDecorator();
	}
	
	private void playClipAlert()
	{
		Runnable r = new Runnable()
		{
			public void run() 
			{
				File	soundFile = new File(clipWav);
				AudioInputStream	audioInputStream = null;
				try
				{
					audioInputStream = AudioSystem.getAudioInputStream(soundFile);
					AudioFormat	audioFormat = audioInputStream.getFormat();
					DataLine.Info	info = new DataLine.Info(SourceDataLine.class, audioFormat);
					
					SourceDataLine line = (SourceDataLine) AudioSystem.getLine(info);
					line.open(audioFormat);
					line.start();
					
					int	nBytesRead = 0;
					byte[]	abData = new byte[1024];
					while (nBytesRead != -1)
					{
						nBytesRead = audioInputStream.read(abData, 0, abData.length);
						if (nBytesRead >= 0)
							line.write(abData, 0, nBytesRead);
					}
				
					line.drain();
					line.close();
				} catch (Exception e) {}
			}
		};
		Thread t = new Thread(r);
		t.start();
	}
}