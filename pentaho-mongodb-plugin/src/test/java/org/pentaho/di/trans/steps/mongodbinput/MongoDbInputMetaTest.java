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


package org.pentaho.di.trans.steps.mongodbinput;

import org.junit.BeforeClass;
import org.junit.Test;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.encryption.TwoWayPasswordEncoderPluginType;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.util.EnvUtil;
import org.pentaho.di.trans.steps.loadsave.LoadSaveTester;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.FieldLoadSaveValidatorFactory;
import org.pentaho.di.trans.steps.loadsave.validator.ListLoadSaveValidator;
import org.pentaho.di.trans.steps.loadsave.validator.ObjectValidator;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class MongoDbInputMetaTest {
  @BeforeClass
  public static void beforeClass() throws KettleException {
    PluginRegistry.addPluginType( TwoWayPasswordEncoderPluginType.getInstance() );
    PluginRegistry.init();
    String passwordEncoderPluginID = Const.NVL( EnvUtil.getSystemProperty( Const.KETTLE_PASSWORD_ENCODER_PLUGIN ), "Kettle" );
    Encr.init( passwordEncoderPluginID );
  }

  @Test public void testRoundTrips() throws KettleException, NoSuchMethodException, SecurityException {
    Map<String, String> getterMap = new HashMap<String, String>();
    getterMap.put( "hostname", "getHostnames" );
    getterMap.put( "auth_user", "getAuthenticationUser" );
    getterMap.put( "auth_password", "getAuthenticationPassword" );
    getterMap.put( "auth_kerberos", "getUseKerberosAuthentication" );
    getterMap.put( "use_all_replica_members", "getUseAllReplicaSetMembers" );
    getterMap.put( "execute_for_each_row", "getExecuteForEachIncomingRow" );
    getterMap.put( "mongo_fields", "getMongoFields" );
    getterMap.put( "tag_sets", "getReadPrefTagSets" );
    Map<String, String> setterMap = new HashMap<String, String>();
    setterMap.put( "hostname", "setHostnames" );
    setterMap.put( "auth_user", "setAuthenticationUser" );
    setterMap.put( "auth_password", "setAuthenticationPassword" );
    setterMap.put( "auth_kerberos", "setUseKerberosAuthentication" );
    setterMap.put( "use_all_replica_members", "setUseAllReplicaSetMembers" );
    setterMap.put( "execute_for_each_row", "setExecuteForEachIncomingRow" );
    setterMap.put( "mongo_fields", "setMongoFields" );
    setterMap.put( "tag_sets", "setReadPrefTagSets" );

    Map<String, FieldLoadSaveValidator<?>>
        fieldLoadSaveValidatorAttributeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();
    fieldLoadSaveValidatorAttributeMap.put( "tag_sets", new ListLoadSaveValidator<String>(
        new FieldLoadSaveValidator<String>() {

          @Override
          public String getTestObject() {
            return "{" + UUID.randomUUID() + "}";
          }

          @Override
          public boolean validateTestObject( String testObject, Object actual ) {
            return testObject.equals( actual );
          }
        } ) );
    Map<String, FieldLoadSaveValidator<?>>
        fieldLoadSaveValidatorTypeMap =
        new HashMap<String, FieldLoadSaveValidator<?>>();

    LoadSaveTester
        tester =
        new LoadSaveTester( MongoDbInputMeta.class, Arrays.asList( "hostname", "port", "db_name",
            "fields_name", "collection", "json_field_name", "json_query", "auth_user", "auth_password",
            "auth_kerberos", "connect_timeout", "socket_timeout", "read_preference", "output_json",
            "use_all_replica_members", "query_is_pipeline", "execute_for_each_row", "mongo_fields", "tag_sets" ),
            getterMap, setterMap, fieldLoadSaveValidatorAttributeMap, fieldLoadSaveValidatorTypeMap );

    FieldLoadSaveValidatorFactory validatorFactory = tester.getFieldLoadSaveValidatorFactory();

    validatorFactory.registerValidator( validatorFactory.getName( List.class, MongoField.class ),
        new ListLoadSaveValidator<MongoField>( new ObjectValidator<MongoField>( validatorFactory, MongoField.class,
            Arrays.asList( "m_fieldName", "m_fieldPath", "m_kettleType", "m_indexedVals" ) ) ) );

    tester.testXmlRoundTrip();
    tester.testRepoRoundTrip();
  }
}
