/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   BinaryFileTailer.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * BinaryFileReader reads a file in binary format one block at a time, until it
 * reaches the end of the file.
 */
public class BinaryFileReader extends AbstractFileReader<ByteBuffer>
{

    private final ByteBuffer bytebuf;


    /**
     * Creates a Tailer for the given file, with a specified buffer size.
     * 
     * @param bufSize
     *            Buffer size
     * @param dataHandler
     *            application-specific data handler
     */
    BinaryFileReader(int bufSize, ITailerDataHandler<ByteBuffer> dataHandler)
    {
        super(bufSize, dataHandler);
        this.bytebuf = ByteBuffer.wrap(inbuf);
    }


    /**
     * Creates and starts a Tailer for the given file.
     * 
     * @param file
     *            the file to follow.
     * @param delayMillis
     *            the delay between checks of the file for new content in
     *            milliseconds.
     * @param bufSize
     *            buffer size.
     * @param position
     *            position object
     * @param dataHandler
     *            application-specific data handler
     * @return The new tailer
     */
    public static BinaryFileReader create(int bufSize,
            ITailerDataHandler<ByteBuffer> dataHandler)
    {
        return new BinaryFileReader(bufSize, dataHandler);
    }


    @Override
    protected void process(RandomAccessFile reader) throws InterruptedException
    {

        FileChannel ch = reader.getChannel();

        try
        {
            long position = this.readPosition.get();
            reader.seek(position);

            // Check the file length to see if it was rotated
            long reader_length = reader.length();

            if (reader_length > position)
            {
                // The file has more content than it did last time
                safeToShutdown = false;
                position = readBlock(ch);
                if (logger.isDebugEnabled())
                    logger.debug(">current position: "
                            + readPosition.toString());

                safeToShutdown = true; // outside the block                
                if (!running)
                    throw new InterruptedException(
                            "Reader has been interrupted");

            }

        }
        catch (InterruptedException e)
        {
            throw e;
        }
        catch (Throwable e)
        {
            if (dataHandler != null)
                dataHandler.onException(e);
        }

    }


    /**
     * Read data blocks. It is up to handler to make sense out of data blocks,
     * parsing, splitting, or transform as needed. It will continue to read
     * until end of stream.
     * 
     * @param channel
     *            The file to read
     * @return The new position after the lines have been read
     * @throws Exception
     */
    private long readBlock(FileChannel channel) throws Exception
    {

        long pos = channel.position();
        long rePos = pos; // position to re-read

        while (running && ((channel.read(bytebuf)) != -1))
        {
            savePositionAndInvokeCallback(bytebuf, rePos = channel.position());
            bytebuf.clear();
        }
 
        return rePos;
    }

}
