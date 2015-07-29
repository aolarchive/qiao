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

import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroBlockStream
{

    protected Logger logger = LoggerFactory.getLogger(this.getClass());
    private BinaryDecoder fileDecoder = null;

    private static final int SCHEMA_BLOCK_SIZE = 8192;

    private Schema schema = null;
    private byte[] blockSync = new byte[DataFileConstants.SYNC_SIZE];
    private byte[] syncBuffer = new byte[DataFileConstants.SYNC_SIZE];

    private long blockSize;
    private boolean availableBlock = false;
    private long recordsInBlock = 0;
    private ByteBuffer blockBuffer;
    private DataBlock block = null;
    private int blockStartPosition;

    /**
     * Initializes the stream by reading the header and parsing the schema from
     * it.
     *
     */
    public AvroBlockStream() throws IOException
    {
        fileDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    }

    /**
     * Reads the schema from the top of avro file
     *
     *
     * @param channel
     * @throws IOException
     */
    public void readSchema(FileChannel channel) throws IOException
    {
        long curPos = channel.position();

        channel.position(0);

        // Read enough for schema
        ByteBuffer buf = ByteBuffer.allocate(SCHEMA_BLOCK_SIZE);

        int bytesRead = channel.read(buf);

        if (bytesRead <= 0)
            throw new IOException("Possibly corrupt file ");

        if (logger.isDebugEnabled())
            logger.debug("schema bytes read:" + bytesRead);

        BinaryDecoder schemaDecoder = DecoderFactory.get().binaryDecoder(
                buf.array(), null);

        _readSchecma(schemaDecoder);

        blockStartPosition = SCHEMA_BLOCK_SIZE
                - schemaDecoder.inputStream().available();

        channel.position(curPos);
    }

    /**
     * Always returns the first block start position in the file after the
     * header is read
     *
     * @return
     */
    public int getBlockStartPosition()
    {
        return blockStartPosition;
    }

    /**
     * Discards the remaining data in the buffer
     *
     */
    public void discardRemaining()
    {
        fileDecoder = DecoderFactory.get().binaryDecoder(new byte[0], null);
    }

    /**
     * Returns true if recordsInBlock and blockSize has already been read from
     * the buffer, false otherwise.
     *
     * @return
     */
    public boolean isBlockInfoRead()
    {
        return recordsInBlock > 0;
    }

    /**
     * Returns the schema of the avro file being processed
     *
     * @return
     */
    public Schema getSchema()
    {
        return this.schema;
    }

    /**
     * Returns the partial block size available in the stream
     *
     * @return
     */
    public int getAvailable()
    {

        int available = 0;
        try
        {
            available = fileDecoder.inputStream().available();
        }
        catch (IOException e)
        {
        }
        return available;
    }

    /**
     * Appends the the buffer to the stream
     *
     * @param data
     * @throws IOException
     */
    public void append(ByteBuffer data) throws IOException
    {

        data.flip();

        byte[] bytesRemaining = new byte[fileDecoder.inputStream().available()];
        byte[] bytesIncoming = new byte[data.limit()];

        data.get(bytesIncoming);

        if (logger.isTraceEnabled())
        {
            logger.trace("Previous incomplete data length : "
                    + bytesRemaining.length);
        }

        fileDecoder.readFixed(bytesRemaining);

        byte[] nb = new byte[bytesRemaining.length + bytesIncoming.length];
        System.arraycopy(bytesRemaining, 0, nb, 0, bytesRemaining.length);
        System.arraycopy(bytesIncoming, 0, nb, bytesRemaining.length,
                bytesIncoming.length);

        fileDecoder = DecoderFactory.get().binaryDecoder(nb, fileDecoder);

        if (logger.isTraceEnabled())
            logger.trace("Total available now : " + nb.length);

    }

    @SuppressWarnings("deprecation")
    protected void _readSchecma(BinaryDecoder schemaDecoder) throws IOException
    {
        byte[] magic = new byte[DataFileConstants.MAGIC.length];

        Map<String, byte[]> meta = new HashMap<String, byte[]>();
        List<String> metaKeyList = new ArrayList<String>();

        schemaDecoder.readFixed(magic);
        long l = schemaDecoder.readMapStart(); // read meta data
        if (l > 0)
        {
            do
            {
                for (long i = 0; i < l; i++)
                {
                    String key = schemaDecoder.readString(null).toString();
                    ByteBuffer value = schemaDecoder.readBytes(null);
                    byte[] bb = new byte[value.remaining()];
                    value.get(bb);
                    meta.put(key, bb);
                    metaKeyList.add(key);
                }
            }
            while ((l = schemaDecoder.mapNext()) != 0);
        }

        schemaDecoder.readFixed(blockSync); // read sync
        if (logger.isDebugEnabled())
        {
            logger.debug("Block Sync Header :" + Arrays.toString(blockSync));
        }

        metaKeyList = Collections.unmodifiableList(metaKeyList);
        schema = Schema.parse(getMetaString(DataFileConstants.SCHEMA, meta),
                false);
    }

    private String getMetaString(String key, Map<String, byte[]> meta)
    {
        byte[] value = meta.get(key);
        if (value == null)
        {
            return null;
        }
        try
        {
            return new String(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks whether the stream has a complete block available
     *
     * @return : true if block is avaible
     * @throws IOException
     */
    public boolean hasNextBlock() throws IOException
    {
        try
        {

            if (logger.isTraceEnabled())
            {
                logger.trace("hasNextBlock " + availableBlock);
                logger.trace("available for block >" + getAvailable());
            }

            if (availableBlock)
                return true;

            // Check if we can read block length and block size
            if (fileDecoder.inputStream().available() < 16)
                return false;

            if (recordsInBlock == 0)
            {
                recordsInBlock = fileDecoder.readLong(); // read block count

                if (logger.isTraceEnabled())
                    logger.trace("recordsInBlock=" + recordsInBlock);

                if (recordsInBlock <= 0)
                    return false;

                blockSize = fileDecoder.readLong(); // read block size

                if (logger.isTraceEnabled())
                    logger.trace("blockSize=" + blockSize);

            }

            if (blockSize > Integer.MAX_VALUE || blockSize < 0)
            {
                throw new IOException(
                        "Block size invalid or too large for this "
                                + "implementation: " + blockSize);
            }

            if (fileDecoder.inputStream().available() < blockSize
                    + DataFileConstants.SYNC_SIZE)
            {
                if (logger.isTraceEnabled())
                    logger.trace("available="
                            + fileDecoder.inputStream().available()
                            + ", needed="
                            + (blockSize + DataFileConstants.SYNC_SIZE));
                return false;
            }

            availableBlock = true;

            block = nextRawBlock(block);
            blockBuffer = block.getAsByteBuffer();

            return true;

        }
        catch (EOFException eof)
        {
            eof.printStackTrace();
            return false;
        }

    }

    /**
     * Returns the next complete avro block
     *
     * @return
     * @throws IOException
     */
    public ByteBuffer nextBlock() throws IOException
    {
        recordsInBlock = 0;
        return blockBuffer;
    }

    private DataBlock nextRawBlock(DataBlock reuse) throws IOException
    {

        if (reuse == null || reuse.data.length < (int) blockSize)
        {
            reuse = new DataBlock(recordsInBlock, (int) blockSize);
        }
        else
        {
            reuse.numEntries = recordsInBlock;
            reuse.blockSize = (int) blockSize;
        }

        fileDecoder.readFixed(reuse.data, 0, reuse.blockSize);
        fileDecoder.readFixed(syncBuffer);

        if (!Arrays.equals(syncBuffer, blockSync))
        {
            if (logger.isTraceEnabled())
            {
                logger.trace("Header Sync :" + Arrays.toString(blockSync));
                logger.trace("Block Sync :" + Arrays.toString(syncBuffer));
            }

            throw new IOException("Invalid block sync!");
        }

        availableBlock = false;
        return reuse;
    }

    static class DataBlock
    {
        private byte[] data;
        private long numEntries;
        private int blockSize;
        private int offset = 0;

        private DataBlock(long numEntries, int blockSize)
        {
            this.data = new byte[blockSize];
            this.numEntries = numEntries;
            this.blockSize = blockSize;
        }

        DataBlock(ByteBuffer block, long numEntries)
        {
            this.data = block.array();
            this.blockSize = block.remaining();
            this.offset = block.arrayOffset() + block.position();
            this.numEntries = numEntries;
        }

        byte[] getData()
        {
            return data;
        }

        long getNumEntries()
        {
            return numEntries;
        }

        int getBlockSize()
        {
            return blockSize;
        }

        ByteBuffer getAsByteBuffer()
        {
            byte[] ret = new byte[data.length];
            System.arraycopy(data, offset, ret, offset, blockSize);
            return ByteBuffer.wrap(ret, offset, blockSize);
        }
    }

}
