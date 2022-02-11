package org.apache.hive.ql.exec.udf.xdr;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.util.ByteUtils;
import org.junit.Test;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hive.serde2.objectinspector.ImmutableBinaryObjectInspector.ImmutableOI;
import static org.junit.Assert.*;

public class UDFMd5Test  extends UDFBaseTest {

  @Override
  public GenericUDF createUDF() {
    return new UDFMd5();
  }

  @Override
  public ObjectInspector[] argumentsOIs() {
    return new ObjectInspector[] { javaStringObjectInspector, javaStringObjectInspector };
  }

  @Test
  public void testUdf_normalTextValue_ok() throws HiveException {
    Text actual = evaluate("19999999999", "hNLJj#@FgPQYygQA");
    assertEquals(new Text("69c98bf19004f74cbdd9acd36d3f51d4"), actual);
    
    actual = evaluate("19999999999");
    assertEquals(new Text("33fbaa69f0c1c931312a92d81bb3eeab"), actual);
  }

  @Test
  public void testUdf_normalBinaryValue_ok() throws HiveException {
    udf.initialize(new ObjectInspector[] { ImmutableOI, javaStringObjectInspector });

    byte[] expected = ByteUtils.loadBytes("0x69c98bf19004f74cbdd9acd36d3f51d4");
    ImmutableBytesWritable actual = evaluate(
      new ImmutableBytesWritable("19999999999".getBytes(), true),
      new Text("hNLJj#@FgPQYygQA"));
    assertArrayEquals(expected, actual.copyBytes());
    
    expected = ByteUtils.loadBytes("0x33fbaa69f0c1c931312a92d81bb3eeab");
    actual = evaluate(new ImmutableBytesWritable("19999999999".getBytes(), true));
    assertArrayEquals(expected, actual.copyBytes());
  }

  @Test
  public void testUdf_abnormalTextValue_ok() throws HiveException {
    Text actual = evaluate(null, null);
    assertNull(actual);

    actual = evaluate("", "");
    assertEquals("d41d8cd98f00b204e9800998ecf8427e", actual.toString()); 
    
    actual = evaluate("");
    assertEquals("d41d8cd98f00b204e9800998ecf8427e", actual.toString());
  }

  @Test
  public void testUdf_abnormalBinaryValue_ok() throws HiveException {
    udf.initialize(new ObjectInspector[] { ImmutableOI, javaStringObjectInspector });

    ImmutableBytesWritable actual = evaluate(null, null);
    assertNull(actual);

    actual = evaluate(
      new ImmutableBytesWritable("".getBytes(), true),
      new Text());
    assertArrayEquals(ByteUtils.loadBytes("0xd41d8cd98f00b204e9800998ecf8427e"), actual.copyBytes());
  }
}
