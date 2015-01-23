/****************************************************************************
 * AOL CONFIDENTIAL INFORMATION
 *
 * Copyright (c) 2013 AOL Inc.  All Rights Reserved.
 * Unauthorized reproduction, transmission, or distribution of
 * this software is a violation of applicable laws.
 *
 ****************************************************************************
 * Department:  AOL Advertising
 *
 * File Name:   FileWatchService.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file.watcher;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aol.advertising.qiao.util.CommonUtils;

/**
 * FileWatchService watches a directory for registered events: ENTRY_CREATE,
 * ENTRY_DELETE, or ENTRY_MODIFY. It notifies listeners whenever detecting a
 * registered event occurs.
 */
public class FileWatchService implements Runnable
{
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private WatchService watcherService;
    private FileSystem fileSystem = FileSystems.getDefault();
    private String watchPathname;
    private WatchEvent.Kind< ? >[] watchEvents;
    private Path dirPath;
    private WatchKey watchKey;
    private volatile Thread me;
    private List<IFileEventListener> fileEventListeners = Collections
            .synchronizedList(new ArrayList<IFileEventListener>());


    public void init() throws IOException
    {
        watcherService = fileSystem.newWatchService();
        dirPath = fileSystem.getPath(watchPathname);
        watchKey = dirPath.register(watcherService, watchEvents);

        logger.info(this.getClass().getName() + " initialized to watch " + watchPathname);
    }


    public void start()
    {
        me = new Thread(this);
        me.setName(CommonUtils.resolveThreadName(this.getClass()
                .getSimpleName()));
        me.start();

        logger.info(this.getClass().getName() + " started");
    }


    public void run()
    {
        while (true)
        {
            WatchKey key;
            try
            {
                key = watcherService.take();
            }
            catch (InterruptedException e)
            {
                break;
            }

            for (WatchEvent< ? > event : key.pollEvents())
            {
                WatchEvent.Kind< ? > kind = event.kind();

                // events may have been lost or discarded. 
                if (kind == OVERFLOW)
                {
                    logger.info("events may have been lost/discarded");
                    continue;
                }

                // The filename is the context of the event.
                WatchEvent<Path> ev = (WatchEvent<Path>) event;
                Path filename = ev.context();
                if (kind == ENTRY_MODIFY)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("<ENTRY_MODIFY> " + filename + " modified");
                    notifyListenersOnModify(filename);
                }
                else if (kind == ENTRY_CREATE)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("<ENTRY_CREATE> " + filename + " created");
                    notifyListenersOnCreate(filename);
                }
                else if (kind == ENTRY_DELETE)
                {
                    if (logger.isDebugEnabled())
                        logger.debug("<ENTRY_DELETE> " + filename + " deleted");
                    notifyListenersOnDelete(filename);
                }

            }

            // Reset the key -- this step is critical if you want to
            // receive further watch events.  If the key is no longer valid,
            // the directory is inaccessible so exit the loop.
            boolean valid = key.reset();
            if (!valid)
            {
                break;
            }
        }

        logger.info(this.getClass().getName() + " terminated");

    }


    private void notifyListenersOnCreate(Path file)
    {
        Path p = dirPath.resolve(file.getFileName());

        for (IFileEventListener listener : fileEventListeners)
        {
            listener.onCreate(p);
        }
    }


    private void notifyListenersOnDelete(Path file)
    {
        for (IFileEventListener listener : fileEventListeners)
        {
            listener.onDelete(file);
        }
    }


    private void notifyListenersOnModify(Path file)
    {
        for (IFileEventListener listener : fileEventListeners)
        {
            listener.onModify(file);
        }
    }


    public void stop()
    {
        if (me != null)
        {
            watchKey.cancel();

            Thread t = me;
            me = null;
            t.interrupt();
        }
    }


    /**
     * @param dirPathname
     *            directory path to watch
     * @param events
     *            one or more events to watch
     * @throws IOException
     */
    public void setWatchingPath(String dirPathname,
            WatchEvent.Kind< ? >... events) throws IOException
    {
        this.watchPathname = dirPathname;
        this.watchEvents = events;
    }


    /**
     * @param listener
     *            listener to be notified when detecting a registered event
     *            occurs.
     */
    public void registerListener(IFileEventListener listener)
    {
        this.fileEventListeners.add(listener);
    }
}
