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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TextBlockSplitter
{
    Logger logger = LoggerFactory.getLogger(this.getClass());
    volatile List<String> result;
    String remainingBuffer;
    StringBuilder sb = new StringBuilder();
    int positionAdjustment = 0;


    public List<String> splitLines(String inData) throws IOException
    {

        StringReader data;
        if (remainingBuffer != null && remainingBuffer.length() > 0)
            data = new StringReader(remainingBuffer + inData);
        else
            data = new StringReader(inData);

        result = new ArrayList<String>();
        sb.setLength(0);

        int ch;
        boolean seenCR = false;
        while ((ch = data.read()) != -1)
        {
            switch (ch)
            {
                case '\n':
                    seenCR = false; // swallow CR before LF
                    result.add(sb.toString());
                    sb.setLength(0);
                    break;
                case '\r':
                    if (seenCR)
                        sb.append('\r');
                    seenCR = true;
                    break;
                default:
                    if (seenCR)
                    {
                        seenCR = false; // swallow final CR
                        result.add(sb.toString());
                        sb.setLength(0);
                    }
                    sb.append((char) ch); // add character, not its ascii value
            }
        }

        if (sb.length() > 0)
        {
            remainingBuffer = sb.toString();
            positionAdjustment = -remainingBuffer.length();
        }
        else
        {
            remainingBuffer = null;
            positionAdjustment = 0;
        }

        return result;
    }


    public int getPositionAdjustment()
    {
        return positionAdjustment;
    }

}
