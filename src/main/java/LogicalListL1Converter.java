import static org.apache.parquet.schema.Type.Repetition.REPEATED;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * First level of LOGICAL LIST conversion. Handles 'list'
 */
public class LogicalListL1Converter extends GroupConverter  {
  private static final Logger logger = LoggerFactory.getLogger(LogicalListL1Converter.class);
  /**
   * Checks if the schema is similar to the following:
   * <pre>
   * optional group <name> (LIST) {
   *   repeated group <list-name> {
   *     <element-repetition> <element-type> <element-name>;
   *   }
   * }
   * </pre>
   *
   * @param schema parquet group type
   * @return true is supported
   */
  public static boolean isSupportedSchema(GroupType schema) {
    if (schema.getFieldCount() == 1) {
      Type type = schema.getType(0);
      // check: repeated group
      if (type.isPrimitive() || !type.isRepetition(REPEATED) || type.getOriginalType() != null) {
        return false;
      }
      return type.asGroupType().getFieldCount() == 1;
    }
    return false;
  }

  @Override
  public Converter getConverter(final int fieldIndex) {
    return null;
  }

  @Override
  public void start() {

  }

  @Override
  public void end() {

  }
}