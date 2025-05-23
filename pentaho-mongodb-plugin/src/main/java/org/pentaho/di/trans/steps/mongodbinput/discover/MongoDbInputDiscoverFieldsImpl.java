/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2025 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/

package org.pentaho.di.trans.steps.mongodbinput.discover;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.BSONTimestamp;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;
import org.bson.types.Symbol;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.mongo.wrapper.field.MongoField;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;


/**
 * Created by bryan on 8/7/14.
 */
public class MongoDbInputDiscoverFieldsImpl implements MongoDbInputDiscoverFields {
  private static final Class<?> PKG = MongoDbInputDiscoverFieldsImpl.class;

  public List<MongoField> discoverFields( MongoDbInputMeta step, VariableSpace vars, int docsToSample )
    throws KettleException {

    List<MongoField> discoveredFields = new ArrayList<MongoField>();
    Map<String, MongoField> fieldLookup = new HashMap<String, MongoField>();

    String dbName = vars.environmentSubstitute( step.getDbName() );
    String collectionName = vars.environmentSubstitute( step.getCollection() );

    if ( Utils.isEmpty( dbName ) ) {
      throw new KettleException( BaseMessages.getString( PKG,
        "MongoNoAuthWrapper.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
    }

    try ( MongoClient client = step.getMongoClient( vars ) ) {

      MongoCollection<Document> collection = client.getDatabase( dbName ).getCollection( collectionName );

      try ( MongoCursor<Document> cursor =
          step.getMongoCursor( collection, vars, docsToSample < 1 ? 100 : docsToSample, null, null ) ) {

        int actualCount = 0;
        while ( cursor.hasNext() ) {
          actualCount++;
          processRecord( new BasicDBObject( cursor.next() ), "$", "$", fieldLookup );
        }
        postProcessPaths( fieldLookup, discoveredFields, actualCount );

      }

    }

    return discoveredFields;
  }

  protected static void postProcessPaths( Map<String, MongoField> fieldLookup, List<MongoField> discoveredFields,
      int numDocsProcessed ) {
    List<String> fieldKeys = new ArrayList<String>( fieldLookup.keySet() );
    Collections.sort( fieldKeys ); // sorting so name clash number assignments will be deterministic
    for ( String key : fieldKeys ) {
      MongoField m = fieldLookup.get( key );
      m.m_occurenceFraction = "" + m.m_percentageOfSample + "/" + numDocsProcessed;

      setMinArrayIndexes( m );

      // set field names to terminal part and copy any min:max array index info
      if ( m.m_fieldName.contains( "[" ) && m.m_fieldName.contains( ":" ) ) {
        m.m_arrayIndexInfo = m.m_fieldName;
      }
      if ( m.m_fieldName.indexOf( '.' ) >= 0 ) {
        m.m_fieldName = m.m_fieldName.substring( m.m_fieldName.lastIndexOf( '.' ) + 1 );
      }

      if ( m.m_disparateTypes ) {
        // force type to string if we've seen this path more than once
        // with incompatible types
        m.m_kettleType = ValueMetaFactory.getValueMetaName( ValueMetaInterface.TYPE_STRING );
      }
      discoveredFields.add( m );
    }

    // check for name clashes
    Map<String, Integer> tempM = new HashMap<String, Integer>();
    for ( MongoField m : discoveredFields ) {
      if ( tempM.get( m.m_fieldName ) != null ) {
        Integer toUse = tempM.get( m.m_fieldName );
        String key = m.m_fieldName;
        m.m_fieldName = key + "_" + toUse; //$NON-NLS-1$
        toUse = Integer.valueOf( toUse.intValue() + 1 );
        tempM.put( key, toUse );
      } else {
        tempM.put( m.m_fieldName, 1 );
      }
    }
  }

  protected static void setMinArrayIndexes( MongoField m ) {
    // set the actual index for each array in the path to the
    // corresponding minimum index
    // recorded in the name

    if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
      return;
    }

    String temp = m.m_fieldPath;
    String tempComp = m.m_fieldName;
    StringBuffer updated = new StringBuffer();

    while ( temp.indexOf( '[' ) >= 0 ) {
      String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
      String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

      if ( !innerPart.equals( "-" ) ) { //$NON-NLS-1$
        // terminal primitive specific index
        updated.append( temp ); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append( firstPart );

        String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

        if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
          temp = temp.substring( temp.indexOf( ']' ) + 1 );
          tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1 );
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
        String replace = "[" + compParts[0] + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        updated.append( replace );

      }
    }

    if ( temp.length() > 0 ) {
      // append remaining part
      updated.append( temp );
    }

    m.m_fieldPath = updated.toString();
  }


  private static void processRecord( BasicDBObject rec, String path, String name, Map<String, MongoField> lookup ) {
    for ( String key : rec.keySet() ) {
      Object fieldValue = rec.get( key );

      if ( fieldValue instanceof Document ) {
        processRecord( new BasicDBObject( (Document) fieldValue ), path + "." + key, name + "." + key,
          lookup );
      } else if ( fieldValue instanceof BasicDBObject ) {
        processRecord( (BasicDBObject) fieldValue, path + "." + key, name + "." + //$NON-NLS-1$ //$NON-NLS-2$
            key,
          lookup );
      } else if ( fieldValue instanceof BasicDBList ) {
        processList( (BasicDBList) fieldValue, path + "." + key, name + "." + //$NON-NLS-1$ //$NON-NLS-2$
            key,
          lookup );
      } else {
        // some sort of primitive
        String finalPath = path + "." + key; //$NON-NLS-1$
        String finalName = name + "." + key; //$NON-NLS-1$
        if ( !lookup.containsKey( finalPath ) ) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType( fieldValue );
          // Following suit of mongoToKettleType by interpreting null as String type
          newField.m_mongoType = String.class;
          if ( fieldValue != null ) {
            newField.m_mongoType = fieldValue.getClass();
          }
          newField.m_fieldName = finalName;
          newField.m_fieldPath = finalPath;
          newField.m_kettleType = ValueMetaInterface.getTypeDescription( kettleType );
          newField.m_percentageOfSample = 1;

          lookup.put( finalPath, newField );
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get( finalPath );
          Class<?> fieldClass = String.class;
          if ( fieldValue != null ) {
            fieldClass = fieldValue.getClass();
          }
          if ( !m.m_mongoType.isAssignableFrom( fieldClass ) ) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMinMaxArrayIndexes( m, finalName );
        }
      }
    }
  }

  private static void processList( BasicDBList list, String path, String name, Map<String, MongoField> lookup ) {

    if ( list.size() == 0 ) {
      return; // can't infer anything about an empty list
    }

    String nonPrimitivePath = path + "[-]"; //$NON-NLS-1$
    String primitivePath = path;

    for ( int i = 0; i < list.size(); i++ ) {
      Object element = list.get( i );

      if ( element instanceof Document ) {
        processRecord( new BasicDBObject( (Document) element ), nonPrimitivePath, name + "[" + i + ":" + i + "]", 
        lookup );
      } else if ( element instanceof BasicDBObject ) {
        processRecord( (BasicDBObject) element, nonPrimitivePath, name + "[" + i + //$NON-NLS-1$
            ":" + i + "]", //$NON-NLS-1$ //$NON-NLS-2$
          lookup );
      } else if ( element instanceof BasicDBList ) {
        processList( (BasicDBList) element, nonPrimitivePath, name + "[" + i + //$NON-NLS-1$
            ":" + i + "]", //$NON-NLS-1$ //$NON-NLS-2$
          lookup );
      } else {
        // some sort of primitive
        String finalPath = primitivePath + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        String finalName = name + "[" + i + "]"; //$NON-NLS-1$ //$NON-NLS-2$
        if ( !lookup.containsKey( finalPath ) ) {
          MongoField newField = new MongoField();
          int kettleType = mongoToKettleType( element );
          // Following suit of mongoToKettleType by interpreting null as String type
          newField.m_mongoType = String.class;
          if ( element != null ) {
            newField.m_mongoType = element.getClass();
          }
          newField.m_fieldName = finalPath;
          newField.m_fieldPath = finalName;
          newField.m_kettleType = ValueMetaInterface.getTypeDescription( kettleType );
          newField.m_percentageOfSample = 1;

          lookup.put( finalPath, newField );
        } else {
          // update max indexes in array parts of name
          MongoField m = lookup.get( finalPath );
          Class<?> elementClass = String.class;
          if ( element != null ) {
            elementClass = element.getClass();
          }
          if ( !m.m_mongoType.isAssignableFrom( elementClass ) ) {
            m.m_disparateTypes = true;
          }
          m.m_percentageOfSample++;
          updateMinMaxArrayIndexes( m, finalName );
        }
      }
    }
  }

  protected static void updateMinMaxArrayIndexes( MongoField m, String update ) {
    // just look at the second (i.e. max index value) in the array parts
    // of update
    if ( m.m_fieldName.indexOf( '[' ) < 0 ) {
      return;
    }

    if ( m.m_fieldName.split( "\\[" ).length != update.split( "\\[" ).length ) { //$NON-NLS-1$ //$NON-NLS-2$
      throw new IllegalArgumentException( "Field path and update path do not seem to contain " //$NON-NLS-1$
          + "the same number of array parts!" ); //$NON-NLS-1$
    }

    String temp = m.m_fieldName;
    String tempComp = update;
    StringBuffer updated = new StringBuffer();

    while ( temp.indexOf( '[' ) >= 0 ) {
      String firstPart = temp.substring( 0, temp.indexOf( '[' ) );
      String innerPart = temp.substring( temp.indexOf( '[' ) + 1, temp.indexOf( ']' ) );

      if ( innerPart.indexOf( ':' ) < 0 ) {
        // terminal primitive specific index
        updated.append( temp ); // finished
        temp = ""; //$NON-NLS-1$
        break;
      } else {
        updated.append( firstPart );

        String innerComp = tempComp.substring( tempComp.indexOf( '[' ) + 1, tempComp.indexOf( ']' ) );

        if ( temp.indexOf( ']' ) < temp.length() - 1 ) {
          temp = temp.substring( temp.indexOf( ']' ) + 1 );
          tempComp = tempComp.substring( tempComp.indexOf( ']' ) + 1 );
        } else {
          temp = ""; //$NON-NLS-1$
        }

        String[] origParts = innerPart.split( ":" ); //$NON-NLS-1$
        String[] compParts = innerComp.split( ":" ); //$NON-NLS-1$
        int origMin = Integer.parseInt( origParts[0] );
        int compMin = Integer.parseInt( compParts[0] );
        int origMax = Integer.parseInt( origParts[1] );
        int compMax = Integer.parseInt( compParts[1] );

        String newRange =
            "[" + ( compMin < origMin ? compMin : origParts[0] ) + ":" + ( compMax > origMax ? compMax : origParts[1] )
                + "]";
        updated.append( newRange );
      }
    }

    if ( temp.length() > 0 ) {
      // append remaining part
      updated.append( temp );
    }

    m.m_fieldName = updated.toString();
  }

  protected static int mongoToKettleType( Object fieldValue ) {
    if ( fieldValue == null ) {
      return ValueMetaInterface.TYPE_STRING;
    }

    if ( fieldValue instanceof Symbol || fieldValue instanceof String || fieldValue instanceof Code
        || fieldValue instanceof ObjectId || fieldValue instanceof MinKey || fieldValue instanceof MaxKey ) {
      return ValueMetaInterface.TYPE_STRING;
    } else if ( fieldValue instanceof Date ) {
      return ValueMetaInterface.TYPE_DATE;
    } else if ( fieldValue instanceof Number ) {
      // try to parse as an Integer
      try {
        Integer.parseInt( fieldValue.toString() );
        return ValueMetaInterface.TYPE_INTEGER;
      } catch ( NumberFormatException e ) {
        return ValueMetaInterface.TYPE_NUMBER;
      }
    } else if ( fieldValue instanceof Binary ) {
      return ValueMetaInterface.TYPE_BINARY;
    } else if ( fieldValue instanceof BSONTimestamp ) {
      return ValueMetaInterface.TYPE_INTEGER;
    }

    return ValueMetaInterface.TYPE_STRING;
  }

}
