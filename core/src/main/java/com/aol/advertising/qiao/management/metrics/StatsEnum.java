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
