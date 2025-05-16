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

import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

import com.mongodb.BasicDBObject;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;

public class MongoDbInput extends BaseStep implements StepInterface {
  private static final Class<?> PKG = MongoDbInputMeta.class; 

  private MongoDbInputMeta meta;
  private MongoDbInputData data;

  private boolean m_serverDetermined;
  private Object[] m_currentInputRowDrivingQuery = null;

  public MongoDbInput( StepMeta stepMeta, StepDataInterface stepDataInterface, int copyNr, TransMeta transMeta,
      Trans trans ) {
    super( stepMeta, stepDataInterface, copyNr, transMeta, trans );
  }

  @Override 
  public boolean processRow( StepMetaInterface smi, StepDataInterface sdi ) throws KettleException {

    if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery == null ) {
      m_currentInputRowDrivingQuery = getRow();

      if ( m_currentInputRowDrivingQuery == null ) {
        // no more input, no more queries to make
        setOutputDone();
        return false;
      }

      if ( !first ) {
        initQuery();
      }
    }

    if ( first ) {
      first = false;
      
      if ( m_currentInputRowDrivingQuery == null ) {
        data.outputRowMeta = new RowMeta();
      } else {
        data.outputRowMeta = getInputRowMeta().clone();
      }

      meta.getFields( data.outputRowMeta, getStepname(), null, null, this );
      data.init();
      
      initQuery();
      
    }
    
    
    try {

      if ( data.cursor.hasNext() && !isStopped() ) {
        Document nextDoc = data.cursor.next();
        Object[] row = null;

        if ( !meta.getQueryIsPipeline() && !m_serverDetermined ) {
          ServerAddress s = data.cursor.getServerAddress();
          if ( s != null ) {
            m_serverDetermined = true;
            logBasic( BaseMessages.getString( PKG, "MongoDbInput.Message.QueryPulledDataFrom", s.toString() ) ); 
          }
        }

        if ( meta.getOutputJson() || meta.getMongoFields() == null || meta.getMongoFields().isEmpty() ) {
         
          String json = nextDoc.toJson();
          row = RowDataUtil.allocateRowData( data.outputRowMeta.size() );

          if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
            // Add the incoming columns at start and at the end of the incoming columns add json output.
            RowMetaInterface inputRowMeta = getInputRowMeta();
            appendTheIncomingRowsAtStart( row, inputRowMeta );
            row[ inputRowMeta.size() ] = json;
          } else {
            // If there are no incoming columns then adding only json output to output row
            row[ 0 ] = json;
          }
          putRow( data.outputRowMeta, row );
        } else {
          Object[][] outputRows = data.mongoDocumentToKettle( new BasicDBObject( nextDoc ) , this );

          // there may be more than one row if the paths contain an array
          // unwind

          for ( Object[] outputRow : outputRows ) {
            // Add all the incoming column values if they are not null
            if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
              appendTheIncomingRowsAtStart( outputRow, getInputRowMeta() );
            }
            putRow( data.outputRowMeta, outputRow );
          }

        }
      } else {
        if ( !meta.getExecuteForEachIncomingRow() ) {
          setOutputDone();

          return false;
        } else {
          m_currentInputRowDrivingQuery = null; // finished with this row
        }
      }

      return true;
    } catch ( Exception e ) {
      if ( e instanceof KettleException ) {
        throw (KettleException) e;
      } else {
        throw new KettleException( e ); //$NON-NLS-1$
      }
    }
  }

  private void appendTheIncomingRowsAtStart( Object[] row, RowMetaInterface inputRowMeta ) {
    for ( int columnIndex = 0; columnIndex < inputRowMeta.size(); columnIndex++ ) {
      if ( m_currentInputRowDrivingQuery[ columnIndex ] != null ) {
        row[ columnIndex ] = m_currentInputRowDrivingQuery[ columnIndex ];
      }
    }
  }

  protected void initQuery() throws KettleException {

    // close any previous cursor
    if ( data.cursor != null ) {
      data.cursor.close();
    }

    // check logging level and only set to false if
    // logging level at least detailed
    if ( log.isDetailed() ) {
      m_serverDetermined = false;
    }

    String query = environmentSubstitute( meta.getJsonQuery() );
    String fields = environmentSubstitute( meta.getFieldsName() );

    // Executing for each row, perform substitutions
    if ( meta.getExecuteForEachIncomingRow() && m_currentInputRowDrivingQuery != null ) {
      // do field value substitution
      query = fieldSubstitute( query, getInputRowMeta(), m_currentInputRowDrivingQuery );
      fields = fieldSubstitute( fields, getInputRowMeta(), m_currentInputRowDrivingQuery );
    }

    logDetailed( BaseMessages.getString( PKG, "MongoDbInput.Message.ExecutingQuery", query ) );

    if ( meta.getQueryIsPipeline() ) {

      // pipeline aggregations require a query
      if ( Utils.isEmpty( query ) ) {
        throw new KettleException( BaseMessages
            .getString( MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline" ) );
      }

      data.cursor =
          data.collection.aggregate( BsonArray.parse( query ).stream()
              .map( BsonValue::asDocument )
              .map( BsonDocument::toJson )
              .map( Document::parse )
              .collect( Collectors.toList() ) )
              .allowDiskUse( meta.isAllowDiskUse() )
              .cursor();

    } else {

      FindIterable<Document> findCommand = data.collection.find();
      if ( !Utils.isEmpty( query ) ) {
        findCommand = findCommand.filter( BasicDBObject.parse( query ) );
      }
      if ( !Utils.isEmpty( fields ) ) {
        findCommand = findCommand.projection( BasicDBObject.parse( fields ) );
      }
      data.cursor = findCommand.iterator();
    }
  }

  @Override 
  public boolean init( StepMetaInterface stepMetaInterface, StepDataInterface stepDataInterface ) {
    if( !super.init( stepMetaInterface, stepDataInterface ) ) {
      return false;
    }
    
    meta = (MongoDbInputMeta) stepMetaInterface;
    data = (MongoDbInputData) stepDataInterface;

    String db = environmentSubstitute( meta.getDbName() );
    String collection = environmentSubstitute( meta.getCollection() );

    try {
      
      if ( Utils.isEmpty( db ) ) {
        throw new Exception( BaseMessages.getString( PKG, "MongoInput.ErrorMessage.NoDBSpecified" ) ); //$NON-NLS-1$
      }

      if ( Utils.isEmpty( collection ) ) {
        throw new Exception( BaseMessages.getString( PKG, "MongoInput.ErrorMessage.NoCollectionSpecified" ) ); //$NON-NLS-1$
      }

      if ( !Utils.isEmpty( meta.getAuthenticationUser() ) ) {
        String
            authInfo =
            ( meta.getUseKerberosAuthentication() ? BaseMessages
                .getString( PKG, "MongoDbInput.Message.KerberosAuthentication",
                    environmentSubstitute( meta.getAuthenticationUser() ) ) : BaseMessages
                    .getString( PKG, "MongoDbInput.Message.NormalAuthentication",
                      environmentSubstitute( meta.getAuthenticationUser() ) ) );

        logBasic( authInfo );
      }

      // init connection constructs a MongoCredentials object if necessary
      data.client = meta.getMongoClient( this );
      data.collection = data.client.getDatabase( db ).getCollection( collection );

      if ( !meta.getOutputJson() ) {
        data.setMongoFields( meta.getMongoFields() );
      }

      return true;
      
    } catch ( Exception e ) {
      logError( BaseMessages.getString( PKG, "MongoDbInput.ErrorConnectingToMongoDb.Exception", db, collection ), e );
      return false;
    }
  }

  @Override 
  public void dispose( StepMetaInterface smi, StepDataInterface sdi ) {
    if ( data.cursor != null ) {
      try {
        data.cursor.close();
      } catch ( Exception e ) {
        log.logError( e.getMessage(), e );
      }
    }
    if ( data.client != null ) {
      try {
        data.client.close();
      } catch ( Exception e ) {
        log.logError( e.getMessage(), e );
      }
    }

    super.dispose( smi, sdi );
  }
}
