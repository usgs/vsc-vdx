package gov.usgs.vdx.data.wave.plot;

/**
 * Customized SliceWaveRenderer
 * $Log: not supported by cvs2svn $
 * @author Dan Cervelli
 */
public class MinimumWaveRenderer extends SliceWaveRenderer
{
	/**
	 * Set axis ticks and labels values 
	 */
	public void update()
	{
		this.setExtents(viewStartTime, viewEndTime, minY, maxY);
		int hTicks = graphWidth / 108;
		int vTicks = graphHeight / 24;
		this.createDefaultAxis(hTicks, vTicks);
		this.setXAxisToTime(hTicks, false);
		this.getAxis().setInnerLeftLabelAsText(yLabel, -46);
		if (title != null)
			this.getAxis().setLeftLabelAsText(title, -56);
	}
}
