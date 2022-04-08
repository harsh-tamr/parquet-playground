import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.eclipse.collections.api.multimap.list.ListMultimap;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.joda.time.DateTimeConstants;

public class ParquetConverter {
  public static Stream<ListMultimap<String, String>> getRecords(ParquetFileReader parquetFileReader) {
    return StreamSupport.stream(new RecordSpliterator(parquetFileReader), false);
  }

  /**
   * Number of days between Julian day epoch (January 1, 4713 BC) and Unix day epoch (January 1, 1970).
   * The value of this constant is {@value}.
   */
  public static final long JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH = 2440588;
  
  /**
   * Utilities for converting from parquet INT96 binary (impala, hive timestamp)
   * to date time value. This utilizes the Joda library.
   */
  public static class NanoTimeUtils {

    public static final long NANOS_PER_MILLISECOND = 1000000;

    /**
     * @param binaryTimeStampValue
     *          hive, impala timestamp values with nanoseconds precision
     *          are stored in parquet Binary as INT96 (12 constant bytes)
     *
     * @return  Unix Timestamp - the number of milliseconds since January 1, 1970, 00:00:00 GMT
     *          represented by @param binaryTimeStampValue .
     */
    public static long getDateTimeValueFromBinary(Binary binaryTimeStampValue) {
      // This method represents binaryTimeStampValue as ByteBuffer, where timestamp is stored as sum of
      // julian day number (32-bit) and nanos of day (64-bit)
      NanoTime nt = NanoTime.fromBinary(binaryTimeStampValue);
      int julianDay = nt.getJulianDay();
      long nanosOfDay = nt.getTimeOfDayNanos();
      return (julianDay - JULIAN_DAY_NUMBER_FOR_UNIX_EPOCH) * DateTimeConstants.MILLIS_PER_DAY
        + nanosOfDay / NANOS_PER_MILLISECOND;
    }
  }

  private static class RecordSpliterator implements Spliterator<ListMultimap<String, String>> {
    private int rowIndex = 0;
    private int pageNumber = 0;
    private MessageType schema;
    private PageReadStore page = null;
    private MessageColumnIO columnIO;
    private GroupRecordConverter groupRecordConverter;
    private RecordReader recordReader = null;
    private final ParquetFileReader reader;
    private boolean init = false;

    public RecordSpliterator(ParquetFileReader reader) {
      this.reader = reader;
    }

    private boolean readPage() {
      try {
//        logger.debug("Source: {}, Reading Page: {}", reader.getFile(), pageNumber);
        page = reader.readNextRowGroup();
        pageNumber++;
        if (page == null) {
          return false;
        }
        recordReader = columnIO.getRecordReader(page, groupRecordConverter);
        rowIndex = 0;
      } catch (IOException e) {
//        logger.error(e.getMessage(),e);
      }
      return true;
    }


    @Override
    public boolean tryAdvance(Consumer<? super ListMultimap<String, String>> action) {
      if (!init) {
        schema = reader.getFileMetaData().getSchema();
        columnIO = new ColumnIOFactory().getColumnIO(schema);
        groupRecordConverter = new GroupRecordConverter(schema);
        init = true;
        readPage();
      }
      if (page == null) return false;
      if (rowIndex == page.getRowCount()) {
        if (!readPage()) return false;
      }
      final ListMultimap<String, String> record = getRecord();
      action.accept(record);
      rowIndex++;
      return true;
    }

    public ListMultimap<String, String> getRecord() {
      SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
      final MutableListMultimap<String, String> record = Multimaps.mutable.list.empty();
      final List<String> columns = schema.getColumns().stream().map(columnDescriptor -> String.join(".", columnDescriptor.getPath())).collect(
        Collectors.toList());

      for (int fieldIndex = 0; fieldIndex < schema.getFieldCount(); fieldIndex++) {
        final String fieldName = columns.get(fieldIndex);
        if (simpleGroup.getFieldRepetitionCount(fieldIndex) == 0) {
          record.put(fieldName, null);
        } else {
          try {
            addToField(schema, simpleGroup.getGroup(fieldIndex, 0), record, fieldName);
          } catch (Exception e) {
            addPlainTypeAsString(simpleGroup, record, fieldName, schema, fieldIndex, 0);
          }
        }
      }
      return record;
    }

    private static void addPlainTypeAsString(Group group, MutableListMultimap<String, String> record, String fieldName, MessageType schema,
      int fieldIndex, int repetitonIndex) {
      try {
        if (schema.getColumns().get(10).getPrimitiveType().toString().contains(" int96 ")) {
          record.put(fieldName, Instant.ofEpochMilli(NanoTimeUtils.getDateTimeValueFromBinary(group.getInt96(fieldIndex, repetitonIndex))).toString());
        } else {
          record.put(fieldName, group.getValueToString(fieldIndex, repetitonIndex));
        }

//        if (type.getPrimitiveType().equals(PrimitiveType.DECIMAL) && type.getScale() + type.getPrecision() <= 16) {
//          field.add(DecimalUtility.getBigDecimalFromByteBuffer(simpleGroup.getBinary(fieldIndex, 0).toByteBuffer(), type.getScale()).toPlainString());
//        } else if(type.getPrimitiveType().equals(PrimitiveType.TIMESTAMP)){
//          field.add(new SimpleDateFormat("dd-MM-yyyy HH:mm:ss.SSS z").format(new Date(
//            TimeUnit.MILLISECONDS.convert(simpleGroup.getLong(fieldIndex, 0), TimeUnit.MICROSECONDS))));
//        } else {
//          field.add(simpleGroup.getValueToString(fieldIndex, 0));
//        }
      } catch (Exception e) {
        record.put(fieldName, group.getValueToString(fieldIndex, repetitonIndex));
      }
    }
//
    private static void addToField(final MessageType schema, Group group, MutableListMultimap<String, String> record,
      String fieldName) {
      if (group.getFieldRepetitionCount(0) == 0) {
        record.put(fieldName, null);
      }
      for (int repetitionIndex = 0; repetitionIndex < group.getFieldRepetitionCount(0); repetitionIndex++) {
        try {
          addToField(schema, group.getGroup(0, repetitionIndex), record, fieldName);
        } catch (Exception e) {
          addPlainTypeAsString(group, record, fieldName, schema, 0, repetitionIndex);
        }
      }
    }

    @Override
    public Spliterator<ListMultimap<String, String>> trySplit() {
      return null;
    }

    @Override
    public long estimateSize() {
      return 0;
    }

    @Override
    public int characteristics() {
      return Spliterator.NONNULL | Spliterator.IMMUTABLE | Spliterator.ORDERED;
    }
  }



  //  private static final LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<String> baseLogicalTypeAnnotationVisitor = new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<String>() {
//    @Override
//    public Optional<String> visit(final ListLogicalTypeAnnotation listLogicalType) {
//      return Optional.of("Array<String>");
//    }
//  };

//  public List<Attribute> getAttributes(MessageType messageType) {
//    final List<Attribute> attributes = messageType.convertWith(new TypeConverter<List<Attribute>>() {
//
//      @Override
//      public List<Attribute> convertPrimitiveType(final List<GroupType> list, final PrimitiveType primitiveType) {
//        if (primitiveType.getLogicalTypeAnnotation() != null) {
//          final String type = primitiveType.getLogicalTypeAnnotation().accept(baseLogicalTypeAnnotationVisitor).orElse(null);
//          if (type != null) {
//            return Collections.singletonList(new Attribute(primitiveType.getName(), type));
//          }
//        }
//      }
//
//      @Override
//      public List<Attribute> convertGroupType(final List<GroupType> list, final GroupType groupType, final List<List<Attribute>> list1) {
//        return null;
//      }
//
//      @Override
//      public List<Attribute> convertMessageType(final MessageType messageType, final List<List<Attribute>> list) {
//        return null;
//      }
//    });
//    return attributes;
//  }
//
//  public class Attribute {
//    public String name;
//    public String type;
//    public int depth;
//
//    public Attribute(final String name, final String type) {
//      this.name = name;
//      this.type = type;
//      this.depth = 0;
//    }
//  }
//
//  public static List<Attribute> messageTypeToSchema(MessageType schema) {
//    final List<Attribute> parquetAttributes = schema.convertWith(new TypeConverter<List<Attribute>>() {
//      public List<Attribute> createFromType(Type type, Attribute attributeType, Integer depth) {
//        return Collections.singletonList(new ParquetAttribute(type.getName(), type.getName(), attributeType, depth));
//      }
//
//      @Override
//      public List<> convertPrimitiveType(List<GroupType> path, org.apache.parquet.schema.PrimitiveType primitiveType) {
//        Attribute attributeType = null;
//        final int size = path.size();
//        if (primitiveType.getLogicalTypeAnnotation() != null) {
//          final Optional<PrimitiveType> fieldType = primitiveType.getLogicalTypeAnnotation().accept(baseLogicalTypeAnnotationVisitor);
//          //    TODO Original Types https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
//          if (fieldType.isPresent()) {
//            attributeType = AttributeFactory.fromPrimitive(fieldType.get(), !primitiveType.isRepetition(Type.Repetition.REQUIRED));
//          }
//        }
//        if (attributeType == null) {
//          switch (primitiveType.getPrimitiveTypeName()) {
//            //      TODO: Singletons?
//            case FLOAT:
//              attributeType = AttributeFactory.FLOAT;
//              break;
//            case INT32:
//              attributeType = AttributeFactory.INTEGER;
//              break;
//
//            case INT64:
//              attributeType = AttributeFactory.BIGINT;
//              break;
//
//            case DOUBLE:
//              attributeType = AttributeFactory.DOUBLE;
//              break;
//
//            case BOOLEAN:
//              attributeType = AttributeFactory.BOOLEAN;
//              break;
//            case BINARY:
//            case INT96:
//            case FIXED_LEN_BYTE_ARRAY:
//              attributeType = AttributeFactory.BLOB;
//              break;
//          }
//        }
//        if (primitiveType.getRepetition().equals(Type.Repetition.REPEATED)) {
//          return Collections.singletonList(
//            new ParquetAttribute(primitiveType.getName(), primitiveType.getName(), new Attribute(PrimitiveType.ARRAY, attributeType, false),
//              size));
//        }
//        return createFromType(primitiveType, attributeType, size);
//      }
//
//      @Override
//      public List<ParquetAttribute> convertGroupType(List<GroupType> path, GroupType groupType, List<List<ParquetAttribute>> children) {
//        Integer leafDepth = children.get(0).get(0).getMaxDepth();
//        //  1      repeated group key_value { -> string[]
//        //  2        required binary key (STRING); -> string
//        //  3        optional group value (MAP) { -> string?
//        //  4          repeated group key_value { -> string
//        //  5            required int32 key;
//        //  6            required boolean value;
//        //            }
//        //          }
//        //        }
//        if (leafDepth > 3 || children.size() > 1 || children.get(0).size() > 1) {
//          return Collections.singletonList(new ParquetAttribute(groupType.getName(), groupType.getName(),
//            AttributeFactory.fromPrimitive(PrimitiveType.LONGVARCHAR, !groupType.isRepetition(Type.Repetition.REQUIRED)), leafDepth));
//        }
//
//        // List<Integer> (nullable list, non-null elements)
//        //        optional group my_list (LIST) {
//        //          repeated int32 element;
//        //        }
//
//        // 1 List<String> (list non-null, elements nullable)
//        // 2       required group my_list (LIST) {
//        // 3         repeated group list {
//        // 4           optional binary element (UTF8);
//        //          }
//        //        }
//        //        3
//        if (groupType.getRepetition() == Type.Repetition.REPEATED && childIsSinglePrimitive(children)) {
//          return Collections.singletonList(new ParquetAttribute(groupType.getName(), groupType.getName(),
//            new Attribute(PrimitiveType.ARRAY, children.get(0).get(0).getType(), children.get(0).get(0).getType().getNullable()), leafDepth));
//        }
//        //        2
//        if (childIsSingleArrayPrimitive(children)) {
//          return Collections.singletonList(new ParquetAttribute(groupType.getName(), groupType.getName(),
//            new Attribute(PrimitiveType.ARRAY, children.get(0).get(0).getType().getInnerType(),
//              !groupType.isRepetition(Type.Repetition.REQUIRED)), leafDepth));
//        }
//
//        //        Catch all
//        return Collections.singletonList(new ParquetAttribute(groupType.getName(), groupType.getName(),
//          AttributeFactory.fromPrimitive(PrimitiveType.LONGVARCHAR, !groupType.isRepetition(Type.Repetition.REQUIRED)), leafDepth));
//      }
//
//      private boolean childIsSinglePrimitive(List<List<ParquetAttribute>> children) {
//        return notATuple(children) && children.get(0).get(0).getType().getInnerType() == null
//          && children.get(0).get(0).getType().getPrimitiveType() != null;
//      }
//
//      private boolean notATuple(List<List<ParquetAttribute>> children) {
//        return children.size() == 1 && children.get(0).size() == 1;
//      }
//
//      private boolean childIsSingleArrayPrimitive(List<List<ParquetAttribute>> children) {
//        return children.size() == 1 && children.get(0).size() == 1 && children.get(0).get(0).getType().getPrimitiveType().equals(PrimitiveType.ARRAY)
//          && children.get(0).get(0).getType().getInnerType() != null;
//      }
//
//      @Override
//      public List<ParquetAttribute> convertMessageType(MessageType messageType, List<List<ParquetAttribute>> children) {
//        return children.stream().flatMap(Collection::stream).collect(Collectors.toList());
//      }
//    });
//    return parquetAttributes.stream().map(type -> (IAttribute) type).collect(Collectors.toList());
//  }

}
