package com.aol.advertising.qiao.management;

import java.util.HashMap;
import java.util.Map;

import com.aol.advertising.qiao.injector.file.watcher.FileWatchService;
import com.aol.advertising.qiao.injector.file.watcher.QiaoFileManager;

// This is a singleton
public class ToolsStore
{
    private final Map<String, QiaoFileManager> fileManagerMap;
    private final Map<String, QiaoFileBookKeeper> bookKeeperMap;
    private final Map<String, FileWatchService> fileWatcherMap;

    private static ToolsStore instance;


    protected ToolsStore()
    {
        fileManagerMap = new HashMap<>();
        bookKeeperMap = new HashMap<>();
        fileWatcherMap = new HashMap<>();
    }


    private static void _createInstanceIfNeeded()
    {
        if (null == instance)
        {
            instance = new ToolsStore();
        }
    }


    public static void addFileManager(String agentId,
            QiaoFileManager fileManager)
    {

        _createInstanceIfNeeded();
        instance.fileManagerMap.put(agentId, fileManager);
    }


    public static void addBookKeeper(String agentId, QiaoFileBookKeeper bk)
    {
        _createInstanceIfNeeded();
        instance.bookKeeperMap.put(agentId, bk);
    }


    public static void addFileWatch(String agentId,
            FileWatchService fileWatcher)
    {
        _createInstanceIfNeeded();
        instance.fileWatcherMap.put(agentId, fileWatcher);
    }


    public static QiaoFileManager getFileManager(String agentId)
    {
        if (null == instance)
            return null;

        return instance.fileManagerMap.get(agentId);
    }


    public static QiaoFileBookKeeper getBookKeeper(String agentId)
    {
        if (null == instance)
            return null;

        return instance.bookKeeperMap.get(agentId);
    }


    public static FileWatchService getFileWatcher(String agentId)
    {
        if (null == instance)
            return null;

        return instance.fileWatcherMap.get(agentId);

    }


    public static void removeFileManager(String agentId)
    {
        if (null == instance)
            return;

        instance.fileManagerMap.remove(agentId);
    }


    public static void removeBookKeeper(String agentId)
    {
        if (null == instance)
            return;

        instance.bookKeeperMap.remove(agentId);
    }


    public static void removeFileWatcher(String agentId)
    {
        if (null == instance)
            return;

        instance.fileWatcherMap.remove(agentId);

    }
}
