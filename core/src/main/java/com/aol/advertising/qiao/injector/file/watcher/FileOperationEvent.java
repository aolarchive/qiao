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
