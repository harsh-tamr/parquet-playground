import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class LoadParquet {

  public static final DatumReader<GenericRecord> DATUM_READER = new GenericDatumReader<>();
  public static final GenericData GENERIC_DATA = GenericData.get();

  public static void main(String[] args) throws IOException, InterruptedException {
//    final String avroFile = args[0];
//    final String bucket = args[1];
//    final String path = args[2];
//    writeAvroToParquet(avroFile, bucket, path);
//    readParquet("alltypes_dictionary");
//        readParquet("nested_lists.snappy");
//    readParquet("custom2_after_tamr_export");
//    writeAvroToParquet("/Users/hisingh1/projects/export-parquet/custom.avro","tamr-core-connect-test", "export/custom.parquet");
//    writeAvroToParquet("/Users/hisingh1/projects/export-parquet/arrays.avro","tamr-core-connect-test", "export/arrays.parquet");
    writeAvroToParquet("/Users/hisingh1/projects/export-parquet/CUSTOMER_LEGAL_MASTERING_unified_dataset_dedup_published_clusters_with_data.avro","tamr-core-connect-test", "export/CUSTOMER_LEGAL_MASTERING_unified_dataset_dedup_published_clusters_with_data.parquet");
//    readParquet("arrays");

  }

  private static void readParquet(final String name) throws IOException {
    ParquetConverter.getRecords(ParquetFileReader.open(
      HadoopInputFile.fromPath(new Path("file:///Users/hisingh1/projects/export-parquet/parquet-variety/" + name + ".parquet"),
        new Configuration()))).forEach(stringStringListMultimap -> System.out.println(stringStringListMultimap));
  }

  private static void writeAvroToParquet(final String avroFile, final String bucket, final String path) throws IOException, InterruptedException {
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFile), DATUM_READER);

    final InMemoryOutputFile outputFile = writeToParquet(dataFileReader);
    final String file = "./" + path;
    Files.write(Paths.get(file), outputFile.toArray());
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
    System.out.println("done writing file");
    TransferManager tm = TransferManagerBuilder.standard()
      .withS3Client(s3)
      .withMultipartUploadThreshold((long) (5 * 1024 * 1025))
      .withExecutorFactory(() -> Executors.newFixedThreadPool(16))
      .build();
    final Upload upload = tm.upload(bucket, path, new File(file));
    upload.waitForCompletion();
    System.out.println("done");
    tm.shutdownNow();
  }

  public static <T extends SpecificRecordBase> InMemoryOutputFile writeToParquet(DataFileReader<GenericRecord> dataFileReader) throws IOException {
    Schema avroSchema = dataFileReader.getSchema();
    GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.DateConversion());
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    Configuration conf = new Configuration();
    conf.setBoolean("parquet.avro.write-old-list-structure", false);
    try (ParquetWriter<Object> writer = AvroParquetWriter.builder(outputFile)
      .withDataModel(GENERIC_DATA)
      .withSchema(avroSchema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.SNAPPY)
      .withWriteMode(ParquetFileWriter.Mode.CREATE)
      .build()) {
      dataFileReader.iterator().forEachRemaining(r -> {
        try {
//          System.out.println(r);
          writer.write(r);
        } catch (IOException ex) {
          throw new UncheckedIOException(ex);
        }
      });
    } catch (IOException e) {
      e.printStackTrace();
    }
    return outputFile;
  }
}
