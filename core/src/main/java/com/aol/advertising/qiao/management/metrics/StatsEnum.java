/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2011 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   StatsEnum.java	
 * Description:
 * @author:     ytung
 * @version:    1.0
 *
 ****************************************************************************/

package com.aol.advertising.qiao.management.metrics;

public enum StatsEnum
{
    // cumulative counters
    EVENT_INPUT_COUNT("input_count"), // 
    EVENT_OUTPUT_COUNT("output_count"), // 
    PROCESSED_COUNT("processed_count"), //
    PROCESSED_FILES("processed_files"), //
    PROCESSED_BLOCKS("processed_blocks"), //
    // average counters
    INTVL_AVG_INPUT("intvl_avg_input_count"), // 
    INTVL_AVG_OUTPUT("intvl_avg_output_count"), // 
    INTVL_AVG_PROCESSED("intvl_avg_processed_count"), // 
    // gauge
    DATAPIPE_QUEUELEN("queue_length"), //
    // interval metrics
    FILE_PROCESSING_TIME("file_processing_time"), //
    EMIT_TIME("emit_time") //
    ;

    public static final String AVERAGE = "intvl_avg_";

    private String value;


    private StatsEnum(String value)
    {
        this.value = value;
    }


    public static StatsEnum find(String name)
    {
        for (StatsEnum t : values())
        {
            if (t.value.equals(name))
            {
                return t;
            }
        }

        return null;
    }


    public String value()
    {
        return value;
    }

}
