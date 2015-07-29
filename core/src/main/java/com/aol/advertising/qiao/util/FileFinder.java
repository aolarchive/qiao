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

package com.aol.advertising.qiao.util;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileFinder extends SimpleFileVisitor<Path>
{

    private Logger logger = LoggerFactory.getLogger(this.getClass());
    private final PathMatcher matcher;
    private List<Path> matchedFiles = new ArrayList<Path>();
    private int maxFiles = 0;


    public FileFinder(String pattern)
    {
        matcher = FileSystems.getDefault().getPathMatcher("glob:" + pattern);
    }


    // Compares the glob pattern against the file or directory name.
    void find(Path file)
    {
        Path name = file.getFileName();
        if (name != null && matcher.matches(name))
        {
            matchedFiles.add(file);
        }
    }


    // clear out the result
    public void reset()
    {
        matchedFiles.clear();
    }


    public List<Path> getMatchedFiles()
    {
        return matchedFiles;
    }


    // Invoke the pattern matching method on each file.
    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
    {
        find(file);
        if (maxFiles > 0 && matchedFiles.size() >= maxFiles)
            return TERMINATE;

        return CONTINUE;
    }


    // Invoke the pattern matching method on each directory.
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
    {
        find(dir);
        return CONTINUE;
    }


    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc)
    {
        if (exc instanceof NoSuchFileException)
            return CONTINUE;

        logger.warn(exc.getClass().getName() + ": " + exc.getMessage());
        return CONTINUE;
    }


    public void setMaxFiles(int maxFiles)
    {
        this.maxFiles = maxFiles;
    }


    static void usage()
    {
        System.err
                .println("java FileFind <path>" + " -name \"<glob_pattern>\"");
        System.exit(-1);
    }


    public static void main(String[] args) throws IOException
    {

        if (args.length < 3 || !args[1].equals("-name"))
            usage();

        Path startingDir = Paths.get(args[0]);
        String pattern = args[2];

        FileFinder finder = new FileFinder(pattern);
        Files.walkFileTree(startingDir, finder);
        List<Path> files = finder.getMatchedFiles();
        System.out.println("matched files:");
        for (Path p : files)
            System.out.println(p);
    }

}
