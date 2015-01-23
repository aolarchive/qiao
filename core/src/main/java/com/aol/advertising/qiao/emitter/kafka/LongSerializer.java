/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2014 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   LongSerializer.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.emitter.kafka;

import java.nio.ByteBuffer;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

public class LongSerializer implements Encoder<Long>
{
    private static ByteBuffer buffer = ByteBuffer.allocate(8);

    // required for scala signature
    public LongSerializer(VerifiableProperties props)
    {        
    }
    
    @Override
    public byte[] toBytes(Long value)
    {
        buffer.putLong(0, value);
        return buffer.array();
    }

}
