package com.aol.advertising.qiao.util;

/**
* Class to generate timestamps with microsecond precision
* For example: MicroTimestamp.INSTANCE.get() = "2012-10-21 19:13:45.267128"
*/ 
public enum MicroTimestamp 
{
	INSTANCE;

	private volatile long startDate;
	private volatile long startNanoseconds;
	
	private MicroTimestamp() 
	{
		this.startDate = System.currentTimeMillis();
		this.startNanoseconds = System.nanoTime();
	}

	public long currentMicroSeconds()
	{
		long nanoSpan = System.nanoTime() - this.startNanoseconds;
		
		if (nanoSpan < 0)
		{
			synchronized(this)
			{
				nanoSpan = System.nanoTime() - this.startNanoseconds;
				if (nanoSpan < 0)
				{
					this.startDate = System.currentTimeMillis();
					this.startNanoseconds = System.nanoTime();
					nanoSpan = System.nanoTime() - this.startNanoseconds;
				}
			}
		}
		return (this.startDate * 1000 + nanoSpan/1000);
	}
}
