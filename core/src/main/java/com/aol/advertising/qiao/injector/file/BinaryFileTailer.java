/****************************************************************************
 * Copyright (c) 2015 AOL Inc.
 * @author:     ytung05
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import com.aol.advertising.qiao.management.FileReadingPositionCache;
import com.aol.advertising.qiao.util.CommonUtils;

/**
 * BinaryFileTailer reads a file in binary format one block at a time.
 *
 */
public class BinaryFileTailer extends AbstractFileTailer<ByteBuffer>
{

    private final ByteBuffer bytebuf;


    /**
     * Creates a Tailer for the given file, with a specified buffer size.
     *
     * @param file
     *            the file to follow.
     * @param delayMillis
     *            the delay between checks of the file for new content in
     *            milliseconds.
     * @param bufSize
     *            Buffer size
     * @param position
     *            position object
     * @param dataHandler
     *            application-specific data handler
     */
    BinaryFileTailer(File file, long delayMillis, int bufSize,
            FileReadingPositionCache position,
            ITailerDataHandler<ByteBuffer> dataHandler)
    {
        super(file, delayMillis, bufSize, position, dataHandler);
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
    public static BinaryFileTailer create(File file, long delayMillis,
            int bufSize, FileReadingPositionCache position,
            ITailerDataHandler<ByteBuffer> dataHandler)
    {
        BinaryFileTailer tailer = new BinaryFileTailer(file, delayMillis,
                bufSize, position, dataHandler);

        return tailer;
    }


    @Override
    protected boolean process(RandomAccessFile reader, long pullDelayMillis,
            boolean fileRotated) throws InterruptedException
    {

        FileChannel ch = reader.getChannel();
        boolean file_rotation_detected = false;

        try
        {
            long position = this._position.get();
            if (fileRotated)
            {
                _position.set(0);
                position = 0;
                logger.info(">read from offset 0");
            }
            reader.seek(position);

            boolean file_roated = false;
            while (running && !file_rotation_detected)
            {

                // Check the file length to see if it was rotated
                long reader_length = reader.length();

                if (reader_length < position)
                {

                    // file truncated - start from the beginning
                    logFileTruncation(reader_length, position);
                    _position.set(0);
                    position = 0;
                    reader.seek(position);
                    position = readBlock(ch);
                    if (logger.isDebugEnabled())
                        logger.debug(">current position: "
                                + _position.toString());

                }
                else if (reader_length > position)
                {
                    // The file has more content than it did last time
                    position = readBlock(ch);
                    if (logger.isDebugEnabled())
                        logger.debug(">current position: "
                                + _position.toString());
                }

                // ------ check file rotation ------

                boolean has_new_file = hasNewFile(tailedFile,
                        _position.getReadState().checksum);
                if (has_new_file)
                {
                    // try one more time
                    logger.info(">> Seeing a new file.  Checking one more time to make sure we process everything on current file");
                    position = readBlock(ch);
                    logger.info(">> file ending position: "
                            + _position.toString()
                            + ", Going to the new file...");
                    file_roated = true;
                }

                if (file_roated)
                {
                    file_rotation_detected = true;
                }
                else
                {
                    // wait for new content
                    CommonUtils.sleepQuietly(pullDelayMillis); // avoid tight loop
                }

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

        return file_rotation_detected;
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

        safeToShutdown = false;

        try
        {
            long pos = channel.position();
            long rePos = pos; // position to re-read

            //int num;
            while (running && ((channel.read(bytebuf)) != -1))
            {
                savePositionAndInvokeCallback(bytebuf,
                        rePos = channel.position());
                bytebuf.clear();
            }

            return rePos;
        }
        finally
        {
            safeToShutdown = true;
            if (!running)
                throw new InterruptedException("Reader has been interrupted");
        }
    }

}
