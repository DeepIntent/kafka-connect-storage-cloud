/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package io.confluent.connect.s3.format.protoparquet;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import io.confluent.connect.s3.S3SinkConnectorConfig;
import io.confluent.connect.s3.storage.S3ParquetOutputStream;
import io.confluent.connect.s3.storage.S3Storage;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.proto.ProtoParquetWriter;
import org.apache.parquet.proto.ProtoWriteSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class ProtoParquetRecordWriterProvider
        implements RecordWriterProvider<S3SinkConnectorConfig> {
  private static final Logger log = LoggerFactory.getLogger(
          ProtoParquetRecordWriterProvider.class);
  private static final String EXTENSION = ".parquet";
  private static final int PAGE_SIZE = 4 * 1024 * 1024;
  private final S3Storage storage;
  private final String className;

  ProtoParquetRecordWriterProvider(S3Storage storage, String className) {
    this.storage = storage;
    this.className = className;
  }

  @Override
  public String getExtension() {
    return storage.conf().parquetCompressionCodecName().getExtension() + EXTENSION;
  }

  @Override
  public RecordWriter getRecordWriter(final S3SinkConnectorConfig conf, final String filename) {
    return new RecordWriter() {
      ParquetWriter<Message> writer;
      S3ParquetOutputFile s3ParquetOutputFile;
      Class<? extends Message> messageClass;

      @Override
      public void write(SinkRecord record) {
        if (messageClass == null) {
          try {
            messageClass = Class.forName(className).asSubclass(Message.class);
          } catch (ClassNotFoundException e) {
            throw new ConnectException(e);
          }
        }

        if (writer == null) {
          try {
            log.info("Opening record writer for: {}", filename);
            s3ParquetOutputFile = new S3ParquetOutputFile(storage, filename);
            writer = ProtoParquetWriterBuilder.<Message>builder(s3ParquetOutputFile)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withDictionaryEncoding(true)
                    .withCompressionCodec(storage.conf().parquetCompressionCodecName())
                    .withPageSize(PAGE_SIZE)
                    .withRowGroupSize(storage.conf().getPartSize())
                    .withMessage(messageClass)
                    .build();

          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        try {
          log.trace("Sink record: {}", record.toString());
          Method method = messageClass.getDeclaredMethod("parseFrom", byte[].class);
          Message value = (Message) method.invoke(null, (byte[]) record.value());

          writer.write(value);
        } catch (IOException
               | NoSuchMethodException
                | IllegalAccessException
                | InvocationTargetException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {
        try {
          writer.close();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          s3ParquetOutputFile.s3out.setCommit();
          if (writer != null) {
            writer.close();
          }
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }
    };
  }


  private static class S3ParquetOutputFile implements OutputFile {
    private static final int DEFAULT_BLOCK_SIZE = 0;
    private S3Storage storage;
    private String filename;
    private S3ParquetOutputStream s3out;

    S3ParquetOutputFile(S3Storage storage, String filename) {
      this.storage = storage;
      this.filename = filename;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
      s3out = (S3ParquetOutputStream) storage.create(filename, true);
      return s3out;
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return DEFAULT_BLOCK_SIZE;
    }
  }


  private static class ProtoParquetWriterBuilder<T extends MessageOrBuilder>
          extends ProtoParquetWriter<T> {

    public ProtoParquetWriterBuilder(Path file, Class<? extends Message> protoMessage,
                                     CompressionCodecName compressionCodecName,
                                     int blockSize, int pageSize) throws IOException {
      super(file, protoMessage, compressionCodecName, blockSize, pageSize);
    }

    public static <T> Builder<T> builder(Path file) {
      return new Builder<T>(file);
    }

    public static <T> Builder<T> builder(OutputFile file) {
      return new Builder<T>(file);
    }

    private static <T extends MessageOrBuilder> WriteSupport<T> writeSupport(
            Class<? extends Message> protoMessage) {
      return new ProtoWriteSupport<T>(protoMessage);
    }

    public static class Builder<T> extends ParquetWriter.Builder<T, Builder<T>> {

      Class<? extends Message> protoMessage = null;

      private Builder(Path file) {
        super(file);
      }

      private Builder(OutputFile file) {
        super(file);
      }

      protected Builder<T> self() {
        return this;
      }

      public Builder<T> withMessage(Class<? extends Message> protoMessage) {
        this.protoMessage = protoMessage;
        return this;
      }

      @Override
      protected WriteSupport<T> getWriteSupport(Configuration conf) {
        return (WriteSupport<T>) ProtoParquetWriterBuilder.writeSupport(protoMessage);
      }
    }

  }

}
