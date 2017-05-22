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

package com.aol.advertising.qiao.agent;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jmx.export.annotation.ManagedAttribute;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.aol.advertising.qiao.injector.file.watcher.FileWatchService;
import com.aol.advertising.qiao.injector.file.watcher.QiaoFileManager;
import com.aol.advertising.qiao.management.QiaoFileBookKeeper;
import com.aol.advertising.qiao.management.QiaoFileEntry;
import com.aol.advertising.qiao.management.metrics.StatsManager;
import com.aol.advertising.qiao.util.ContextUtils;

/**
 * QiaoAgent contains one or more data funnels based on qiao.xml. When the agent
 * starts, all the funnels are started at the time.
 *
 */
@ManagedResource
public class QiaoAgent implements IAgent
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private List<IFunnel> funnelList;
    private StatsManager statsManager;
    private QiaoFileBookKeeper bookKeeper;
    private QiaoFileManager fileManager;
    private boolean enableFileWatcher = true;
    private FileWatchService fileWatcher;

    private AtomicBoolean isSuspended = new AtomicBoolean(false);


    @Override
    public void init() throws Exception
    {
        statsManager = ContextUtils.getBean(StatsManager.class);

        statsManager.init();

        if (bookKeeper != null)
        {
            bookKeeper.init();

            fileManager.setBookKeeper(bookKeeper);
            fileManager.init();

            _setupFileWatcher();
        }

        for (IFunnel funnel : funnelList)
        {
            funnel.init();
        }

        logger.info(this.getClass().getName() + " initialized");
    }


    private void _setupFileWatcher() throws IOException, ClassNotFoundException
    {
        if (!enableFileWatcher)
            return;

        fileWatcher = ContextUtils.getBean(FileWatchService.class);
        fileWatcher.registerListener(fileManager);

        String dir_path = fileManager.getSrcDir();
        if (dir_path != null)
        {
            fileWatcher.setWatchingPath(dir_path,
                    java.nio.file.StandardWatchEventKinds.ENTRY_CREATE,
                    java.nio.file.StandardWatchEventKinds.ENTRY_DELETE);
        }

        fileWatcher.init();
    }


    @Override
    public void start() throws Exception
    {
        // if (positionCache != null)
        //    positionCache.start();

        if (fileManager != null)
            fileManager.start();

        if (fileWatcher != null)
            fileWatcher.start();

        for (IFunnel funnel : funnelList)
            funnel.start();

        statsManager.start();

        logger.info(this.getClass().getName() + " started");

        logger.info("\n   ------------\n   QIAO started\n   ------------");

    }


    @Override
    public void shutdown()
    {
        for (IFunnel funnel : funnelList)
            funnel.close();

        statsManager.shutdown();

        if (bookKeeper != null)
            bookKeeper.close();

        if (fileManager != null)
            fileManager.shutdown();

        if (fileWatcher != null)
            fileWatcher.stop();

        //if (positionCache != null)
        //    positionCache.close();

        logger.info(this.getClass().getName() + " shutdown");

    }


    @Override
    public void setFunnels(List<IFunnel> funnelList)
    {
        this.funnelList = funnelList;
    }


    public void setStatsManager(StatsManager statsManager)
    {
        this.statsManager = statsManager;
    }


    @ManagedOperation
    @Override
    public void suspend()
    {
        if (isSuspended.compareAndSet(false, true))
        {
            for (IFunnel funnel : funnelList)
                funnel.suspend();

            if (fileWatcher != null)
                fileWatcher.stop();

            if (fileManager != null)
                fileManager.suspend();

            statsManager.suspend();

            logger.info("==> Agent suspended");
        }
    }


    @ManagedOperation
    @Override
    public void resume()
    {
        if (isSuspended.compareAndSet(true, false))
        {
            try
            {
                if (fileManager != null)
                    fileManager.start();

                if (fileWatcher != null)
                    fileWatcher.start();

                statsManager.resume();

            }
            catch (Exception e)
            {
                logger.error(
                        "failed to start QiaoFileManager: " + e.getMessage(), e);
                isSuspended.set(true);
                return;
            }

            for (IFunnel funnel : funnelList)
                funnel.resume();

            logger.info("==> Agent resumed");

        }

    }


    public void setBookKeeper(QiaoFileBookKeeper bookKeeper)
    {
        this.bookKeeper = bookKeeper;
    }

    @Override
    public void setFileManager(final QiaoFileManager fileManager) {
        this.fileManager = fileManager;
    }

    @ManagedAttribute
    @Override
    public boolean isSuspended()
    {
        return isSuspended.get();
    }


    @ManagedOperation
    public String dumpBookKeeperCache()
    {
        return bookKeeper.dumpHistoryCache();
    }


    @ManagedOperation
    public String removeBookKeeperCacheEntry(long checksum)
    {
        QiaoFileEntry entry = bookKeeper.deleteHistory(checksum);
        if (entry == null)
            return "No entry for " + checksum + " to delete";

        return "Success";
    }


    @ManagedOperation(description = "Reset statistics counters")
    public void resetStatsCounters()
    {
        logger.info("reset counters...");
        statsManager.resetCounters();
    }

}
