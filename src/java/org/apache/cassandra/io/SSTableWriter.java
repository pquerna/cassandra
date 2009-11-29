package org.apache.cassandra.io;
/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */


import java.io.File;
import java.io.IOException;
import java.io.FileOutputStream;
import java.io.DataOutputStream;
import java.util.Comparator;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.BloomFilter;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DatabaseDescriptor.CompressionMethod;
import com.reardencommerce.kernel.collections.shared.evictable.ConcurrentLinkedHashMap;

public class SSTableWriter extends SSTable
{
    private static Logger logger = Logger.getLogger(SSTableWriter.class);

    private long keysWritten;
    private BufferedRandomAccessFile dataFile;
    private BufferedRandomAccessFile indexFile;
    private DecoratedKey lastWrittenKey;
    private BloomFilter bf;

    public SSTableWriter(String filename, long keyCount, IPartitioner partitioner) throws IOException
    {
        super(filename, partitioner);
        dataFile = new BufferedRandomAccessFile(path, "rw", (int)(DatabaseDescriptor.getFlushDataBufferSizeInMB() * 1024 * 1024));
        indexFile = new BufferedRandomAccessFile(indexFilename(), "rw", (int)(DatabaseDescriptor.getFlushIndexBufferSizeInMB() * 1024 * 1024));
        bf = new BloomFilter((int)keyCount, 15); // TODO fix long -> int cast
    }

    private long beforeAppend(DecoratedKey decoratedKey) throws IOException
    {
        if (decoratedKey == null)
        {
            throw new IOException("Keys must not be null.");
        }
        Comparator<DecoratedKey> c = partitioner.getDecoratedKeyComparator();
        if (lastWrittenKey != null && c.compare(lastWrittenKey, decoratedKey) > 0)
        {
            logger.info("Last written key : " + lastWrittenKey);
            logger.info("Current key : " + decoratedKey);
            logger.info("Writing into file " + path);
            throw new IOException("Keys must be written in ascending order.");
        }
        return (lastWrittenKey == null) ? 0 : dataFile.getFilePointer();
    }

    private void afterAppend(DecoratedKey decoratedKey, long position) throws IOException
    {
        String diskKey = partitioner.convertToDiskFormat(decoratedKey);
        bf.add(diskKey);
        lastWrittenKey = decoratedKey;
        long indexPosition = indexFile.getFilePointer();
        indexFile.writeUTF(diskKey);
        indexFile.writeLong(position);
        if (logger.isTraceEnabled())
            logger.trace("wrote " + decoratedKey + " at " + position);

        if (keysWritten++ % INDEX_INTERVAL != 0)
            return;
        if (indexPositions == null)
        {
            indexPositions = new ArrayList<KeyPosition>();
        }
        indexPositions.add(new KeyPosition(decoratedKey, indexPosition));
        if (logger.isTraceEnabled())
            logger.trace("wrote index of " + decoratedKey + " at " + indexPosition);
    }

    public void append(DecoratedKey decoratedKey, DataOutputBuffer buffer) throws IOException
    {
        append(decoratedKey, buffer.getData(), buffer.getLength());
    }

    public void append(DecoratedKey decoratedKey, byte[] value) throws IOException
    {
        append(decoratedKey, value, value.length);
    }

    public void append(DecoratedKey decoratedKey, byte[] value, int length) throws IOException
    {
        long currentPosition = beforeAppend(decoratedKey);
        dataFile.writeUTF(partitioner.convertToDiskFormat(decoratedKey));
        assert length > 0;
        CompressionMethod cm = DatabaseDescriptor.getSSTableRowCompression();
        if (cm != CompressionMethod.none) {
          int cmid = SSTableRowCompression.cm2id(cm);
          DataOutputBuffer ob = SSTableRowCompression.compress(cm, value, length);
          dataFile.writeInt(cmid);
          dataFile.writeInt(length);
          dataFile.writeInt(ob.getLength());
          dataFile.write(ob.getData());
        }
        else {
          dataFile.writeInt(length);
          dataFile.write(value);
        }
        afterAppend(decoratedKey, currentPosition);
    }

    /**
     * Renames temporary SSTable files to valid data, index, and bloom filter files
     */
    public SSTableReader closeAndOpenReader(double cacheFraction) throws IOException
    {
        // bloom filter
        FileOutputStream fos = new FileOutputStream(filterFilename());
        DataOutputStream stream = new DataOutputStream(fos);
        BloomFilter.serializer().serialize(bf, stream);
        stream.flush();
        fos.getFD().sync();
        stream.close();

        // index
        indexFile.getChannel().force(true);
        indexFile.close();

        // main data
        dataFile.close(); // calls force

        rename(indexFilename());
        rename(filterFilename());
        path = rename(path); // important to do this last since index & filter file names are derived from it

        ConcurrentLinkedHashMap<DecoratedKey, Long> keyCache = cacheFraction > 0
                                                        ? SSTableReader.createKeyCache((int) (cacheFraction * keysWritten))
                                                        : null;
        return new SSTableReader(path, partitioner, indexPositions, bf, keyCache);
    }

    static String rename(String tmpFilename)
    {
        String filename = tmpFilename.replace("-" + SSTable.TEMPFILE_MARKER, "");
        new File(tmpFilename).renameTo(new File(filename));
        return filename;
    }

    public static SSTableReader renameAndOpen(String dataFileName) throws IOException
    {
        SSTableWriter.rename(indexFilename(dataFileName));
        SSTableWriter.rename(filterFilename(dataFileName));
        dataFileName = SSTableWriter.rename(dataFileName);
        return SSTableReader.open(dataFileName, StorageService.getPartitioner(), DatabaseDescriptor.getKeysCachedFraction(parseTableName(dataFileName)));
    }

}
