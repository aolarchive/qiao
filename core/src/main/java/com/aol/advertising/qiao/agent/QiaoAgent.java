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

import java.io.File;
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
import com.aol.advertising.qiao.management.ToolsStore;
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
    private String id; // agent Id

    private List<IFunnel> funnelList;
    private QiaoFileBookKeeper bookKeeper;
    private QiaoFileManager fileManager;
    private boolean enableFileWatcher = true;
    private FileWatchService fileWatcher;

    private AtomicBoolean isSuspended = new AtomicBoolean(false);
    private String historyCacheDir;


    @Override
    public void init() throws Exception
    {

        resolveCacheDirectories();

        if (bookKeeper != null)
        {
            bookKeeper.setHistoryCacheDir(historyCacheDir);
            bookKeeper.init();
            fileManager.setBookKeeper(bookKeeper);
            fileManager.init();

            _setupFileWatcher();
        }

        for (IFunnel funnel : funnelList)
        {
            funnel.setAgentId(id);
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

        ToolsStore.addFileWatch(id, fileWatcher);

        fileWatcher.init();
    }


    @Override
    public void start() throws Exception
    {

        if (fileManager != null)
            fileManager.start();

        if (fileWatcher != null)
            fileWatcher.start();

        for (IFunnel funnel : funnelList)
            funnel.start();

        logger.info(this.getClass().getName() + " started");

        logger.info("\n   ------------\n   Agent " + id
                + " started\n   ------------");

    }


    @Override
    public void shutdown()
    {
        for (IFunnel funnel : funnelList)
            funnel.close();

        if (bookKeeper != null)
        {
            bookKeeper.close();
            ToolsStore.removeBookKeeper(id);
        }

        if (fileManager != null)
        {
            fileManager.shutdown();
            ToolsStore.removeFileManager(id);
        }

        if (fileWatcher != null)
        {
            fileWatcher.stop();
            ToolsStore.removeFileWatcher(id);
        }

        logger.info(this.getClass().getName() + " shutdown");

    }


    @Override
    public void setFunnels(List<IFunnel> funnelList)
    {
        this.funnelList = funnelList;
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

            }
            catch (Exception e)
            {
                logger.error(
                        "failed to start QiaoFileManager: " + e.getMessage(),
                        e);
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
        ToolsStore.addBookKeeper(id, bookKeeper);
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


    public String getId()
    {
        return id;
    }


    public void setId(String id)
    {
        this.id = id;
    }


    public void setFileManager(QiaoFileManager fileManager)
    {
        this.fileManager = fileManager;
        ToolsStore.addFileManager(id, fileManager);
    }


    private void resolveCacheDirectories()
    {
        String sep = File.separator;

        String qiao_home = System.getProperty("qiao.home");
        if (qiao_home == null)
        {
            logger.info(
                    "System property 'qiao.home' not defined.  Set default cache dir to /tmp.");
            qiao_home = sep + "tmp";
        }

        String qiao_cache_dir = qiao_home + sep + "qiao_cache";

        this.historyCacheDir = qiao_cache_dir + sep + "history" + sep + id;
    }

}
