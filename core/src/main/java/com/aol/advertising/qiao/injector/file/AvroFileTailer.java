/****************************************************************************
 * Copyright (c) 2015 AOL Inc.
 * @author:     ashishbh
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
public class AvroFileTailer extends AbstractFileTailer<ByteBuffer>
{
    private final ByteBuffer bytebuf;
    private AvroBlockStream avroStream;

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
    AvroFileTailer(File file, long delayMillis, int bufSize,
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
    public static AvroFileTailer create(File file, long delayMillis,
            int bufSize, FileReadingPositionCache position,
            ITailerDataHandler<ByteBuffer> dataHandler)
    {
        AvroFileTailer tailer = new AvroFileTailer(file, delayMillis, bufSize,
                position, dataHandler);

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

            avroStream = new AvroBlockStream();
            avroStream.readSchema(ch);

            long position = this._position.get();

            if (logger.isDebugEnabled()) {
                logger.debug("fileRotated :" + fileRotated);
                logger.debug("position :" + position);
            }

            if (fileRotated || position == 0)
            {
                // Set to position after header
                position = avroStream.getBlockStartPosition();
                _position.set(position);
                logger.info(">read from offset " + position);
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

                    return true;  // file truncated

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
                    CommonUtils.sleepQuietly(pullDelayMillis); // avoid tight
                                                               // loop
                }

            }

        }
        catch (InterruptedException e)
        {
            logger.error("Interrupted", e);
            throw e;
        }
        catch (Throwable e)
        {
            logger.error("error", e);

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

            logger.trace("channel position=" + pos);

            while (running && ((channel.read(bytebuf)) != -1))
            {
                savePositionAndInvokeCallback(bytebuf,
                        rePos = channel.position());
                bytebuf.clear();

            }
            channel.position(rePos); // // Ensure we can re-read if necessary
            return rePos;
        }
        finally
        {
            safeToShutdown = true;
            if (!running)
                throw new InterruptedException("Reader has been interrupted");
        }
    }

    @Override
    protected int invokeCallback(ByteBuffer data) throws Exception
    {
        try
        {
            avroStream.append(data);

            // Process logical avro blocks in one physical block
            while (avroStream.hasNextBlock())
            {
                super.callback.receive(avroStream.nextBlock());
                //ByteBuffer b  = avroStream.nextBlock() ;
            }
            // Check if block info was read from the remaining data
            return - (avroStream.getAvailable() + (avroStream.isBlockInfoRead() == true ? 16 : 0));
        }
        catch (Exception e)
        {
            logger.error("data handler throwed exception: " + e.getMessage(), e);
            throw e;
        }
    }

}
