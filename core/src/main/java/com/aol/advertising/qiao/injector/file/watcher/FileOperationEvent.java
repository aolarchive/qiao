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
 * File Name:   FileOperationEvent.java	
 * Description:
 * @author:     ytung05
 *
 ****************************************************************************/

package com.aol.advertising.qiao.injector.file.watcher;

import java.nio.file.Path;

public class FileOperationEvent
{
    public enum EVENT_TYPE
    {
        RENAME_FILE, MOVE_FILE
    };

    public EVENT_TYPE eventType;
    public Path filePath;
    public long checksum;
    public Path newfilePath;


    public FileOperationEvent(EVENT_TYPE type, Path file, long checksum,
            Path newFile)
    {
        this.eventType = type;
        this.filePath = file;
        this.checksum = checksum;
        this.newfilePath = newFile;
    }
}
