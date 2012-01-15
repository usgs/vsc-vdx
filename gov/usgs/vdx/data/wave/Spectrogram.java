package gov.usgs.vdx.data.wave;

import gov.usgs.math.FFT;
import gov.usgs.math.Util;

/**
 * An immutable class for calculating spectrograms. User defines
 * bin size, fft length, and amount of overlap.  The signal is
 * windowed using a Kaiser window with user specifiable beta-value.
 * 
 * @author Peter Cervelli
 */

public class Spectrogram {
	
	public static final int DEFAULT_SAMPLING_RATE = 100;
	public static final int DEFAULT_NFFT = 512;
	public static final int DEFAULT_BIN_SIZE = 256;
	public static final int DEFAULT_OVERLAP = 220;
	public static final double DEFAULT_BETA = 5;
	public static final double DEFAULT_MULTIPLIER = 20;
	public static final double REFERENCE_AMPLITUDE = 1;
	
	private final int samplingRate;
	private final int nfft;
	private final int binSize;
	private final int overlap;
	private final int nRows;
	private final int nColumns;
	private final double beta;	
	private final double[] signal;
	
	private final double[] frequency;
	private final double[] time;
	private final double[] window;	
	private final double[][] spectraAmplitude;
	
	
	/**
	 * Simple constructor that uses default values
	 * 
	 * @param s Signal
	 */
	
	public Spectrogram(double[] s) {
		
		this(s, DEFAULT_SAMPLING_RATE, DEFAULT_NFFT, DEFAULT_BIN_SIZE, DEFAULT_OVERLAP, DEFAULT_BETA);
		
	}
	
	/**
	 * Complete constructor
	 * 
	 * @param s Signal
	 * @param sr Sampling Rate
	 * @param nf FFT length
	 * @param bs Bin size (in samples)
	 * @param ol Overlap (in samples)
	 * @param b Beta value for Kaiser window
	 */
	
	public Spectrogram(double[] s, int sr, int nf, int bs, int ol, double b) {
		
		signal = s;
		samplingRate = sr;
		nfft = nf;
		binSize = bs;
		overlap = ol;
		beta = b;
		
		nRows = nfft/2 + 1;
		nColumns = (int)Math.floor(((double)getNSamples()-(double)overlap)/((double)binSize-(double)overlap));
		frequency = computeFrequency();
		time = computeTime();
		window = Util.kaiser(binSize, beta);
		spectraAmplitude = computeSpectraAmplitude();
		
		System.out.printf("Spectrogram info:\n");
		System.out.printf("\tNumber of samples: %d\n",getNSamples());
		System.out.printf("\tSampling Rate: %d\n",samplingRate);
		System.out.printf("\tN FFT: %d\n",nfft);
		System.out.printf("\tBin Size: %d\n",binSize);
		System.out.printf("\tOverlap: %d\n",overlap);
		System.out.printf("\tBeta: %f\n",beta);
		System.out.printf("\tN rows: %d\n",nRows);
		System.out.printf("\tN columns: %d\n",nColumns);

	}
	
	/**
	 * Returns number of frequency bins, which is equal to the number of rows in
	 * the "spectraAmplitude" array.
	 */
	public int getNFrequencyBins () {
		
		return nRows;
		
	}
	
	/**
	 * Returns number of time bins, which is equal to the number of columns in
	 * the "spectraAmplitude" array.
	 */
	public int getNTimeBins () {
		
		return nColumns;

	}
	
	/**
	 * Returns the user defined sampling rate.
	 */
	public int getSamplingRate () {
		
		return samplingRate;
		
	}
	
	/**
	 * Returns the number of samples, which is equal to the length of the 
	 * input "signal" array.
	 */
	public int getNSamples () {
		
		return signal.length;
		
	}
	
	/**
	 * Returns the user defined FFT length.
	 */
	public int getNfft () {
		
		return nfft;
		
	}
	
	/**
	 * Returns the user defined bin size (in samples).
	 */
	public int getBinSize() {
		
		return binSize;
		
	}
	
	/**
	 * Returns the user defined value for beta, the parameter for the Kaiser
	 * window.
	 */
	public double getBeta () {
		
		return beta;
		
	}
	
	/**
	 * Returns the user defined overlap size (in samples).
	 */
	public int getOverlap () {
		
		return overlap;
		
	}
	
	/**
	 * Returns an array of frequency values corresponding to the rows of the
	 * "spectraAmplitude" array.
	 */
	public double[] getFrequency () {
		
		return frequency;
		
	}
	
	/**
	 * Returns an array of time values corresponding to the columns of the
	 * "spectraAmplitude" array.
	 */
	public double[] getTime () {
		
		return time;
		
	}
	
	/**
	 * Returns the values of the window function used in the spectral calculation.
	 */
	public double[] getWindow () {
		
		return window;
		
	}
	
	/**
	 * Returns the minimum value of the "spectraAmplitude" array.
	 */
	public double getMinSpectraAmplitude() {

		double MIN = Double.MAX_VALUE;
		for (int i = 0; i < nRows; i++)
			for (int j = 0; j < nColumns; j++)
				if (spectraAmplitude[i][j] < MIN)
					MIN = spectraAmplitude[i][j];
		return MIN;
		
	}
	
	/**
	 * Returns the maximum value of the "spectraAmplitude" array.
	 */
	public double getMaxSpectraAmplitude() {
		
		double MAX = Double.MIN_VALUE;
		for (int i = 0; i < nRows; i++)
			for (int j = 0; j < nColumns; j++)
				if (spectraAmplitude[i][j] > MAX)
					MAX = spectraAmplitude[i][j];
		return MAX;
		
	}
	
	/**
	 * Returns an array of spectra amplitudes.
	 */
	public double[][] getSpectraAmplitude () {
		
		return spectraAmplitude;
		
	}
	
	/**
	 * Returns an array of log10 of the spectra amplitudes times
	 * the default multiplier.
	 */
	public double[][] getLogSpectraAmplitude() {
		
		return getLogSpectraAmplitude(DEFAULT_MULTIPLIER, REFERENCE_AMPLITUDE);
		
	}
	
	/**
	 * Returns an array of log10 of the scaled spectra amplitudes times
	 * the specified multiplier.
	 * 
	 * @param multiplier Multiplier
	 */
	public double[][] getLogSpectraAmplitude(double multiplier, double reference_amplitude) {
	
		double[][] logAmp = new double[nRows][nColumns];
		for (int i = 0; i < nRows; i++)
			for (int j = 0; j < nColumns; j++)
				logAmp[i][j] = multiplier * Math.log10(spectraAmplitude[i][j]/reference_amplitude);
		
		return logAmp;
	}
	
	/**
	 * Computes the frequency array
	 */
	private double[] computeFrequency() {

		double[] omega = new double[nRows];
		double delta = (double)samplingRate / (double)(nfft);
		for (int i=0;i<omega.length;i++)
			omega[i] = i * delta;
		return omega;
	}
	
	/**
	 * Computes the time array
	 */
	private double[] computeTime() {
		
		double[] T = new double[nColumns];
		double delta = ((double)binSize - (double)overlap) / (double)samplingRate;
		for (int i=0;i<T.length;i++)
			T[i] = i * delta;	
		return T;		
	}	
	
	/**
	 * Computes the spectra amplitudes with the FFT.
	 */
	private double[][] computeSpectraAmplitude() {

		double[][] specAmp = new double[nRows][nColumns];
		double[][] bin = new double[nfft][2];
		
		int c = 0;
		for (int i = 0; i < nColumns; i++) {
			
			for (int j = 0; j < binSize; j++) {
				bin[j][0] = signal[c] * window[j];
				bin[j][1] = 0;
				c++;
			}			
			c = c - overlap;
			FFT.fft(bin);
			for (int j = 0; j < nfft; j++) {
				if (j < nRows)
					specAmp[j][i] = Math.sqrt(bin[j][0]*bin[j][0] + bin[j][1]*bin[j][1]);
				bin[j][0] = 0;
				bin[j][1] = 0;
			}
		}
		
		return specAmp;
	}

}