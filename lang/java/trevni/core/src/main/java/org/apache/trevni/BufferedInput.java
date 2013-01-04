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
package org.apache.trevni;

import java.io.IOException;

/** An wrapper of {@link Input} for using user-defined buffer size */
public class BufferedInput implements Input {

  private Input internalInput;
  private byte[] buf;
  private int limit;
  private long offset;   // position of Input.
                         // bytes of Input from offset to
                         // offset + limit (excluded) are buffered

  BufferedInput(Input input, int capacity) throws IOException {
    this.internalInput = input;
    this.buf = new byte[capacity];
    this.limit = 0;
    this.offset = 0;
  }

  @Override
  public long length() throws IOException { return internalInput.length(); }

  @Override
  public int read(long position, byte[] b, int start, int len)
    throws IOException {
    int pos;          // position within buffer

    // buffer contains valid data
    if (position >= offset && position < offset + limit) {
      pos = (int)(position - offset);
      int buffered = limit - pos;
      if (buffered < len) { // buffer is insufficient
        System.arraycopy(buf, pos, b, start, buffered); // consume buffer
        start += buffered;
        len -= buffered;
        pos += buffered;
        offset = position + buffered;
        if (len > buf.length) {                     // bigger than buffer
          // read directly into result
          return internalInput.read(offset, b, start, len) + buffered;
        }

        // fill the entire buffer
        limit = internalInput.read(offset, buf, 0, buf.length);
        pos = 0;
      }
    } else {
      // fill the entire buffer
      int read = internalInput.read(position, buf, 0, buf.length);
      offset = position;
      pos = 0;
      limit = read;   // set the limit as the number of bytes read
      if (read < len) {
        len = read;
      }
    }
    System.arraycopy(buf, pos, b, start, len); // copy from buffer
    return len;
  }

  @Override
  public void close() throws IOException { internalInput.close(); }

}

