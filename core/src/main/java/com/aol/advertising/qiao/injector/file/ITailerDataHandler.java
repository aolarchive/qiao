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

import java.util.Iterator;

import com.aol.advertising.qiao.management.FileReadingPositionCache;

public interface ITailerDataHandler<T>
{

    /**
     * Handle and/or transform a record or a block of data from a Tailer.
     * <p>
     * <b>Note:</b> this is called from a tailer thread.
     *
     * @param data
     *            a text line or a blob of data block.
     */
    public Iterator< ? > onData(T data) throws Exception;


    /**
     * @return the position adjustment to current read position so the end of the
     *         last processed logical record can be identified.
     */
    public int getPositionAdjustment();


    /**
     * Initialization. The tailer will call this method during initialization
     * phase, giving the handler a method of stopping the tailer. It is also a
     * place to reposition the read cursor if needed.
     *
     * The following code snippet is a sample implementation that resets the
     * tailer's read position when the file last processed was older than
     * current date:
     *
     * <pre>
     * FileReadState state = tailer.getFileReadState();
     * Date last_date = new Date(state.timestamp);
     * boolean sameday = DateUtils.isSameDay(last_date, new Date());
     * if (!sameday)
     * {
     *     state.position = 0;
     *     logger.info(&quot;Last processed file has been rolled off. Start processing the current file...&quot;);     *
     *     return state;
     * }
     * return null;
     * </pre>
     *
     * @param tailer
     * @return a new state object to override the existing one, null otherwise.
     * @throws Exception
     */
    public FileReadingPositionCache.FileReadState init(IReader<T> tailer)
            throws Exception;


    /**
     * Close all the resources allocated by this instance. The tailer will call
     * this method during shutdown process.
     */
    public void close();


    /**
     * Handles an Exception .
     * <p>
     * <b>Note:</b> this is called from the tailer thread.
     *
     * @param ex
     *            the exception.
     */
    public void onException(Throwable ex);


    /**
     * This method is called if the tailed file is not found.
     * <p>
     * <b>Note:</b> this is called from the tailer thread.
     */
    public void fileNotFound();


    /**
     * Called if a file rotation is detected.
     *
     * This method is called before the file is reopened, and fileNotFound may
     * be called if the new file has not yet been created.
     * <p>
     * <b>Note:</b> this is called from the tailer thread.
     */
    public void fileRotated();
}
