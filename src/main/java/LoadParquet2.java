//import com.amazonaws.services.s3.AmazonS3;
//import com.amazonaws.services.s3.AmazonS3ClientBuilder;
//import com.amazonaws.services.s3.model.PutObjectRequest;
//import com.amazonaws.services.s3.transfer.TransferManager;
//import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
//import com.amazonaws.services.s3.transfer.Upload;
//import java.io.File;
//import java.io.IOException;
//import java.io.UncheckedIOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.concurrent.Executors;
//import org.apache.avro.Schema;
//import org.apache.avro.data.TimeConversions;
//import org.apache.avro.file.DataFileReader;
//import org.apache.avro.generic.GenericData;
//import org.apache.avro.generic.GenericDatumReader;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.DatumReader;
//import org.apache.avro.specific.SpecificRecordBase;
//import org.apache.parquet.avro.AvroParquetWriter;
//import org.apache.parquet.hadoop.ParquetFileWriter;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.hadoop.util.HadoopOutputFile;
//import org.apache.parquet.io.OutputFile;
//
//public class LoadParquet2 {
//
//  public static final DatumReader<GenericRecord> DATUM_READER = new GenericDatumReader<>();
//  public static final GenericData GENERIC_DATA = GenericData.get();
//
//  public static void main(String[] args) throws IOException, InterruptedException {
//    final String avroFile = args[0];
//    final String bucket = args[1];
//    final String path = args[2];
//    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new File(avroFile), DATUM_READER);
//    GENERIC_DATA.addLogicalTypeConversion(new TimeConversions.DateConversion());
//    try (ParquetWriter<Object> writer = AvroParquetWriter.builder(HadoopOutputFile.fromPath())
//      .withDataModel(GENERIC_DATA)
//      .withSchema(dataFileReader.getSchema())
//      .withCompressionCodec(CompressionCodecName.SNAPPY)
//      .withWriteMode(ParquetFileWriter.Mode.CREATE)
//      .build()) {
//      dataFileReader.iterator().forEachRemaining(r -> {
//        try {
//          System.out.println(r);
//          System.out.println(r.get(0));
//          System.out.println(r.get(1));
//          writer.write(r);
//        } catch (IOException ex) {
//          throw new UncheckedIOException(ex);
//        }
//      });
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    final String file = "./yolo.parquet";
//    Files.write(Paths.get(file), outputFile.toArray());
//    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
//    System.out.println("done writing file");
//    TransferManager tm = TransferManagerBuilder.standard()
//      .withS3Client(s3)
//      .withMultipartUploadThreshold((long) (5 * 1024 * 1025))
//      .withExecutorFactory(() -> Executors.newFixedThreadPool(16))
//      .build();
//    final Upload upload = tm.upload(bucket, path, new File(file));
//    upload.waitForCompletion();
//    System.out.println("done");
//    tm.shutdownNow();
//  }
//
//  public static <T extends SpecificRecordBase> InMemoryOutputFile writeToParquet(DataFileReader<GenericRecord> dataFileReader) throws IOException {
//    Schema avroSchema = dataFileReader.getSchema();
//
//    InMemoryOutputFile outputFile = new InMemoryOutputFile();
//    try (ParquetWriter<Object> writer = AvroParquetWriter.builder(outputFile)
//      .withDataModel(GENERIC_DATA)
//      .withSchema(avroSchema)
//      .withCompressionCodec(CompressionCodecName.SNAPPY)
//      .withWriteMode(ParquetFileWriter.Mode.CREATE)
//      .build()) {
//      dataFileReader.iterator().forEachRemaining(r -> {
//        try {
//          System.out.println(r);
//          System.out.println(r.get(0));
//          System.out.println(r.get(1));
//          writer.write(r);
//        } catch (IOException ex) {
//          throw new UncheckedIOException(ex);
//        }
//      });
//    } catch (IOException e) {
//      e.printStackTrace();
//    }
//    return outputFile;
//  }
//}
