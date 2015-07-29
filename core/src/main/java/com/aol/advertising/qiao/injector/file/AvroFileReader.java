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

import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * BinaryFileReader reads a file in binary format one block at a time, until it
 * reaches the end of the file.
 */
public class AvroFileReader extends AbstractFileReader<ByteBuffer>
{

    private final ByteBuffer bytebuf;
    private AvroBlockStream avroStream;

    /**
     * Creates a Tailer for the given file, with a specified buffer size.
     *
     * @param bufSize
     *            Buffer size
     * @param dataHandler
     *            application-specific data handler
     */
    AvroFileReader(int bufSize, ITailerDataHandler<ByteBuffer> dataHandler)
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
    public static AvroFileReader create(int bufSize,
            ITailerDataHandler<ByteBuffer> dataHandler)
    {
        return new AvroFileReader(bufSize, dataHandler);
    }

    @Override
    protected void process(RandomAccessFile reader) throws InterruptedException
    {

        FileChannel ch = reader.getChannel();

        try
        {
            avroStream = new AvroBlockStream();
            avroStream.readSchema(ch);

            long position = this.readPosition.get();

            if (position <= 0 ) {
                // Read header and set position
                position = avroStream.getBlockStartPosition();
                logger.info(">read from offset " + position);
            }

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
            }
           return - (avroStream.getAvailable() + (avroStream.isBlockInfoRead() == true ? 16 : 0));

        }
        catch (Exception e)
        {
            logger.error("data handler throwed exception: " + e.getMessage(), e);
            throw e;
        }
    }
}
