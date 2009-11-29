/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.io;

import org.apache.cassandra.config.DatabaseDescriptor.CompressionMethod;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import java.io.*;

public class SSTableRowCompression
{
    public static int cm2id(CompressionMethod cm)
    {
      switch (cm) {
        case none:
          return 0;
        case gzip:
          return -1;
      }
      return 0;
    }

    public static CompressionMethod id2cm(int id)
    {
      switch (id) {
        case 0:
          return CompressionMethod.none;
        case -1:
          return CompressionMethod.gzip;
      }
      return CompressionMethod.none;
    }
  
  
    public static DataOutputBuffer compress(CompressionMethod cm,
                                            byte[] value, int length) throws IOException
    {
        DataOutputBuffer dob = new DataOutputBuffer();
        
        if (cm == CompressionMethod.gzip) {
          int blen = 1024*32;
          if (length < blen) {
            blen = length;
          }

          byte[] buf = new byte[blen];

          Deflater compresser = new Deflater(Deflater.BEST_SPEED);
          compresser.setInput(value, 0, length);
          compresser.finish();
          while (compresser.finished() != true) {
            int len = compresser.deflate(buf, 0, blen);
            dob.write(buf, 0, len);
          }
        }
        else {
          throw new IOException("invalid compression method");
        }
        return dob;
    }
  
    public static DataInput decompress(CompressionMethod cm,
                                       BufferedRandomAccessFile file,
                                       long offset,
                                       long compressLength,
                                       int expandedSize) throws IOException
    {
      InputStream in = null;

      if (cm == CompressionMethod.gzip) {
        in = new InflaterInputStream(new RandomAccessFileInputStream(file, 
                                                                     offset,
                                                                     compressLength));
      }
      else {
        throw new IOException("invalid compression method");
      }

      return new DataInputStream(in);
    }
}
