/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.mongo.wrapper;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import org.bson.types.Binary;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.row.value.ValueMetaPluginType;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.math.BigDecimal;
import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

public class MongoFieldTest {

  @Mock VariableSpace space;
  private MongoField field;

  @Before
  public void before() throws KettlePluginException {
    MockitoAnnotations.initMocks( this );
    when( space.environmentSubstitute( any( String.class ) ) )
        .thenAnswer( new Answer<String>() {
          @Override public String answer( InvocationOnMock invocationOnMock ) throws Throwable {
            return (String) invocationOnMock.getArguments()[0];
          }
        } );
    PluginRegistry.addPluginType( ValueMetaPluginType.getInstance() );
    PluginRegistry.init();
  }

  @Test
  public void testGetPath() throws Exception {
    MongoField mongoField = new MongoField();

    mongoField.m_fieldPath = "$.parent[0].child[0]";
    assertEquals( "parent.0.child.0", mongoField.getPath() );

    mongoField.m_fieldPath = "$.field[*]";
    assertEquals( "field", mongoField.getPath() );

    mongoField.m_fieldPath = "$.parent.child";
    assertEquals( "parent.child", mongoField.getPath() );
  }

  //"Number", "String", "Date", "Boolean", "Integer", "BigNumber", "Serializable",
  // "Binary", "Timestamp", "Internet Address"
  @Test
  public void testDatatypes() throws KettleException {
    initField( "Number" );
    assertThat( field.getKettleValue( 1.1 ), equalTo( (Object) 1.1 ) );
    assertThat( field.getKettleValue( "1.1" ), equalTo( (Object) 1.1 ) );
    assertThat( field.getKettleValue(
        new Binary( new byte[] { '1', '.', '1' } ) ), equalTo( (Object) 1.1 ) );

    initField( "BigNumber" );
    Date date = new Date();
    assertThat( field.getKettleValue( date ), equalTo( (Object) BigDecimal.valueOf( date.getTime() ) ) );
    assertThat( field.getKettleValue( 12341234 ), equalTo( (Object) BigDecimal.valueOf( 12341234 ) ) );
    assertThat( field.getKettleValue( "12341234" ), equalTo( (Object) BigDecimal.valueOf( 12341234 ) ) );

    initField( "Boolean" );
    assertTrue( (Boolean) field.getKettleValue( 1 ) );
    assertTrue( (Boolean) field.getKettleValue( "Y" ) );
    assertFalse( (Boolean) field.getKettleValue( 0 ) );
    assertTrue( (Boolean) field.getKettleValue( true ) );

    initField( "Binary" );
    byte[] data = new byte[] { 'a', 'b', 'c' };
    assertThat( field.getKettleValue( new Binary( data ) ), equalTo( (Object) data ) );
    assertThat( (byte[]) field.getKettleValue( data ), equalTo( data ) );
    assertThat( field.getKettleValue( "abc" ), equalTo( (Object) data ) );
    
    initField( "Date" );
    assertThat( field.getKettleValue( date ), equalTo( (Object) date ) );
    assertThat( field.getKettleValue( date.getTime() ), equalTo( (Object) date ) );
    try {
      field.getKettleValue( "Not a date value" );
      fail( "expected exception" );
    } catch ( Exception e ) {
      assertThat( e, instanceOf( KettleException.class ) );
    }

    initField( "Integer" );
    assertThat( field.getKettleValue( 123 ), equalTo( (Object) 123l ) );
    assertThat( field.getKettleValue( "123" ), equalTo( (Object) 123l ) );
    assertThat( field.getKettleValue(
        new Binary( new byte[] { '1', '2', '3' } ) ), equalTo( (Object) 123l ) );

    initField( "String" );
    assertThat( field.getKettleValue( "foo" ), equalTo( (Object) "foo" ) );
    assertThat( field.getKettleValue( 123 ), equalTo( (Object) "123" ) );
  }

  @Test
  public void testConvertArrayIndicesToKettleValue() throws KettleException {
    BasicDBObject dbObj = (BasicDBObject) JSON.parse( "{ parent : { fieldName : ['valA', 'valB'] } } " );

    initField( "fieldName", "$.parent.fieldName[0]", "String" );
    assertThat( field.convertToKettleValue( dbObj ), equalTo( (Object) "valA" ) );
    initField( "fieldName", "$.parent.fieldName[1]", "String" );
    assertThat( field.convertToKettleValue( dbObj ), equalTo( (Object) "valB" ) );
  }

  @Test
  public void testConvertUndefinedOrNullToKettleValue() throws KettleException {
    BasicDBObject dbObj = BasicDBObject.parse( "{ test1 : undefined, test2 : null } " );
    initField( "fieldName", "$.test1", "String" );
    //PDI-16090S
    assertNull( "Undefined should be interpreted as null ", field.convertToKettleValue( dbObj ) );
    initField( "fieldName", "$.test2", "String" );
    assertNull( field.convertToKettleValue( dbObj ) );
    initField( "fieldName", "$.test3", "String" );
    assertNull( field.convertToKettleValue( dbObj ) );
  }

  private void initField( String type ) throws KettleException {
    initField( "fieldName", "$.parent.child.fieldName", type );
  }

  private void initField( String name, String path, String type ) throws KettleException {
    field = new MongoField();
    field.m_fieldName = name;
    field.m_fieldPath = path;
    field.m_kettleType = type;
    field.init( 0 );
    field.reset( space );

  }

}
