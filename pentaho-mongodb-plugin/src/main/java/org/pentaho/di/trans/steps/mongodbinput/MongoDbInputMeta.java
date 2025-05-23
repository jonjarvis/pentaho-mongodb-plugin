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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.steps.mongodb.MongoDbMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.mongo.wrapper.field.MongoField;
import org.w3c.dom.Node;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

/**
 * Created on 8-apr-2011
 *
 * @author matt
 * @since 4.2.0-M1
 */
@Step( id = "MongoDbInput", image = "mongodb-input.svg", name = "MongoDB input new",
        description = "Reads from a Mongo DB collection",
        documentationUrl = "mk-95pdia003/pdi-transformation-steps/mongodb-input",
        categoryDescription = "Big Data" )
@InjectionSupported( localizationPrefix = "MongoDbInput.Injection.", groups = ( "FIELDS" ) )
public class MongoDbInputMeta extends MongoDbMeta {
  protected static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes

  @Injection( name = "JSON_OUTPUT_FIELD" )
  private String jsonFieldName;
  @Injection( name = "JSON_FIELD" )
  private String fields;

  @Injection( name = "JSON_QUERY" )
  private String jsonQuery;

  @Injection( name = "AGG_PIPELINE" )
  private boolean m_aggPipeline = false;

  @Injection( name = "ALLOWDISKUSE" )
  private boolean allowDiskUse = false;

  @Injection( name = "OUTPUT_JSON" )
  private boolean m_outputJson = true;

  @InjectionDeep
  private List<MongoField> m_fields;

  @Injection( name = "EXECUTE_FOR_EACH_ROW" )
  private boolean m_executeForEachIncomingRow = false;

  public void setMongoFields( List<MongoField> fields ) {
    m_fields = fields;
  }

  public List<MongoField> getMongoFields() {
    return m_fields;
  }

  public void setExecuteForEachIncomingRow( boolean e ) {
    m_executeForEachIncomingRow = e;
  }

  public boolean getExecuteForEachIncomingRow() {
    return m_executeForEachIncomingRow;
  }

  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    super.loadXML( stepnode, databases, metaStore );
    readData( stepnode );
  }

  @Override
  public Object clone() {
    MongoDbInputMeta retval = (MongoDbInputMeta) super.clone();
    return retval;
  }

  private void readData( Node stepnode ) throws KettleXMLException {
    try {
     
      fields = XMLHandler.getTagValue( stepnode, "fields_name" ); //$NON-NLS-1$
      jsonFieldName = XMLHandler.getTagValue( stepnode, "json_field_name" ); //$NON-NLS-1$
      jsonQuery = XMLHandler.getTagValue( stepnode, "json_query" ); //$NON-NLS-1$

      m_outputJson = true; // default to true for backwards compatibility
      String outputJson = XMLHandler.getTagValue( stepnode, "output_json" ); //$NON-NLS-1$
      if ( !Utils.isEmpty( outputJson ) ) {
        m_outputJson = outputJson.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
      }

      setUseAllReplicaSetMembers( "Y".equalsIgnoreCase( XMLHandler.getTagValue( stepnode, "use_all_replica_members" ) ) ); //$NON-NLS-1$

      String queryIsPipe = XMLHandler.getTagValue( stepnode, "query_is_pipeline" ); //$NON-NLS-1$
      if ( !Utils.isEmpty( queryIsPipe ) ) {
        m_aggPipeline = queryIsPipe.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
      }

      String allowDiskUsage = XMLHandler.getTagValue( stepnode, "allow_disk_use" ); //$NON-NLS-1$
      if ( !Utils.isEmpty( allowDiskUsage ) ) {
        allowDiskUse = allowDiskUsage.equalsIgnoreCase( "Y" ); //$NON-NLS-1$
      }

      String executeForEachR = XMLHandler.getTagValue( stepnode, "execute_for_each_row" );
      if ( !Utils.isEmpty( executeForEachR ) ) {
        m_executeForEachIncomingRow = executeForEachR.equalsIgnoreCase( "Y" );
      }

      Node mongo_fields = XMLHandler.getSubNode( stepnode, "mongo_fields" ); //$NON-NLS-1$
      if ( mongo_fields != null && XMLHandler.countNodes( mongo_fields, "mongo_field" ) > 0 ) { //$NON-NLS-1$
        int nrfields = XMLHandler.countNodes( mongo_fields, "mongo_field" ); //$NON-NLS-1$

        m_fields = new ArrayList<MongoField>();
        for ( int i = 0; i < nrfields; i++ ) {
          Node fieldNode = XMLHandler.getSubNodeByNr( mongo_fields, "mongo_field", i ); //$NON-NLS-1$

          MongoField newField = new MongoField();
          newField.m_fieldName = XMLHandler.getTagValue( fieldNode, "field_name" ); //$NON-NLS-1$
          newField.m_fieldPath = XMLHandler.getTagValue( fieldNode, "field_path" ); //$NON-NLS-1$
          newField.m_kettleType = XMLHandler.getTagValue( fieldNode, "field_type" ); //$NON-NLS-1$
          String indexedVals = XMLHandler.getTagValue( fieldNode, "indexed_vals" ); //$NON-NLS-1$
          if ( indexedVals != null && indexedVals.length() > 0 ) {
            newField.m_indexedVals = MongoDbInputData.indexedValsList( indexedVals );
          }

          m_fields.add( newField );
        }
      }

      String tags = XMLHandler.getTagValue( stepnode, "tag_sets" ); //$NON-NLS-1$
      if ( !Utils.isEmpty( tags ) ) {
        setReadPrefTagSets( new ArrayList<String>() );

        String[] parts = tags.split( "#@#" ); //$NON-NLS-1$
        for ( String p : parts ) {
          getReadPrefTagSets().add( p.trim() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleXMLException( BaseMessages.getString( PKG, "MongoDbInputMeta.Exception.UnableToLoadStepInfo" ), e ); //$NON-NLS-1$
    }
  }

  @Override
  public void setDefault() {
    setHostnames( "localhost" ); //$NON-NLS-1$
    setPort( "27017" ); //$NON-NLS-1$
    setDbName( "db" ); //$NON-NLS-1$
    setCollection( "collection" ); //$NON-NLS-1$
    setUseConnectionString( true ); //$NON-NLS-1$
    setJsonFieldName( "json" );
  }

  @Override
  public void getFields( RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                         VariableSpace space ) throws KettleStepException {

    if ( !m_executeForEachIncomingRow ) {
      // if the "execute for each row" is not checked then we are not allowing rows to pass through this step
      rowMeta.clear();
    }

    if ( m_outputJson || m_fields == null || m_fields.size() == 0 ) {
      ValueMetaInterface jsonValueMeta = new ValueMetaString( jsonFieldName );
      jsonValueMeta.setOrigin( origin );
      rowMeta.addValueMeta( jsonValueMeta );
    } else {
      try {
      for ( MongoField f : m_fields ) {
        ValueMetaInterface vm = ValueMetaFactory.createValueMeta( ValueMetaFactory.getIdForValueMeta( f.m_kettleType ) );
        vm.setName( f.m_fieldName );
        vm.setOrigin( origin );
        if ( f.m_indexedVals != null ) {
          vm.setIndex( f.m_indexedVals.toArray() ); // indexed values
        }
        rowMeta.addValueMeta( vm );
      }
      } catch( KettlePluginException e ) {
        throw new KettleStepException( e );
      }
    }
  }

  protected String tagSetsToString() {
    if ( getReadPrefTagSets() != null && getReadPrefTagSets().size() > 0 ) {
      StringBuilder builder = new StringBuilder();
      for ( int i = 0; i < getReadPrefTagSets().size(); i++ ) {
        String s = getReadPrefTagSets().get( i );
        s = s.trim();
        if ( !s.startsWith( "{" ) ) { //$NON-NLS-1$
          s = "{" + s; //$NON-NLS-1$
        }
        if ( !s.endsWith( "}" ) ) { //$NON-NLS-1$
          s += "}"; //$NON-NLS-1$
        }

        builder.append( s );
        if ( i != getReadPrefTagSets().size() - 1 ) {
          builder.append( "#@#" ); //$NON-NLS-1$
        }
      }
      return builder.toString();
    }
    return null;
  }

  @Override
  public String getXML() {
    StringBuilder retval = new StringBuilder( 300 );

    super.getXML( retval );
    
    retval.append( "    " ).append( XMLHandler.addTagValue( "fields_name", fields ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "json_field_name", jsonFieldName ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "json_query", jsonQuery ) ); //$NON-NLS-1$ //$NON-NLS-2$
    
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "output_json", m_outputJson ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "query_is_pipeline", m_aggPipeline ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
      XMLHandler.addTagValue( "allow_disk_use", allowDiskUse ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "execute_for_each_row", m_executeForEachIncomingRow ) ); //$NON-NLS-1$

    if ( m_fields != null && m_fields.size() > 0 ) {
      retval.append( "\n    " ).append( XMLHandler.openTag( "mongo_fields" ) ); //$NON-NLS-1$ //$NON-NLS-2$

      for ( MongoField f : m_fields ) {
        retval.append( "\n      " ).append( XMLHandler.openTag( "mongo_field" ) ); //$NON-NLS-1$ //$NON-NLS-2$

        retval.append( "\n        " ).append( //$NON-NLS-1$
                XMLHandler.addTagValue( "field_name", f.m_fieldName ) ); //$NON-NLS-1$
        retval.append( "\n        " ).append( //$NON-NLS-1$
                XMLHandler.addTagValue( "field_path", f.m_fieldPath ) ); //$NON-NLS-1$
        retval.append( "\n        " ).append( //$NON-NLS-1$
                XMLHandler.addTagValue( "field_type", f.m_kettleType ) ); //$NON-NLS-1$
        if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
          retval.append( "\n        " ).append( //$NON-NLS-1$
                  XMLHandler.addTagValue( "indexed_vals", //$NON-NLS-1$
                          MongoDbInputData.indexedValsList( f.m_indexedVals ) ) );
        }
        retval.append( "\n      " ).append( XMLHandler.closeTag( "mongo_field" ) ); //$NON-NLS-1$ //$NON-NLS-2$
      }

      retval.append( "\n    " ).append( XMLHandler.closeTag( "mongo_fields" ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    String tags = tagSetsToString();
    if ( !Utils.isEmpty( tags ) ) {
      retval.append( "    " ).append( XMLHandler.addTagValue( "tag_sets", tags ) ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    return retval.toString();
  }

  @Override 
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
          throws KettleException {
    try {
      
      super.readRep( rep, metaStore, id_step, databases );

      fields = rep.getStepAttributeString( id_step, "fields_name" ); //$NON-NLS-1$
      jsonFieldName = rep.getStepAttributeString( id_step, "json_field_name" ); //$NON-NLS-1$
      jsonQuery = rep.getStepAttributeString( id_step, "json_query" ); //$NON-NLS-1$
      m_outputJson = rep.getStepAttributeBoolean( id_step, 0, "output_json" ); //$NON-NLS-1$
      m_aggPipeline = rep.getStepAttributeBoolean( id_step, "query_is_pipeline" ); //$NON-NLS-1$
      allowDiskUse = rep.getStepAttributeBoolean( id_step, "allow_disk_use" ); //$NON-NLS-1$
      m_executeForEachIncomingRow = rep.getStepAttributeBoolean( id_step, "execute_for_each_row" ); //$NON-NLS-1$

      int nrfields = rep.countNrStepAttributes( id_step, "field_name" ); //$NON-NLS-1$
      if ( nrfields > 0 ) {
        m_fields = new ArrayList<MongoField>();

        for ( int i = 0; i < nrfields; i++ ) {
          MongoField newField = new MongoField();

          newField.m_fieldName = rep.getStepAttributeString( id_step, i, "field_name" ); //$NON-NLS-1$
          newField.m_fieldPath = rep.getStepAttributeString( id_step, i, "field_path" ); //$NON-NLS-1$
          newField.m_kettleType = rep.getStepAttributeString( id_step, i, "field_type" ); //$NON-NLS-1$
          String indexedVals = rep.getStepAttributeString( id_step, i, "indexed_vals" ); //$NON-NLS-1$
          if ( indexedVals != null && indexedVals.length() > 0 ) {
            newField.m_indexedVals = MongoDbInputData.indexedValsList( indexedVals );
          }

          m_fields.add( newField );
        }
      }

      String tags = rep.getStepAttributeString( id_step, "tag_sets" ); //$NON-NLS-1$
      if ( !Utils.isEmpty( tags ) ) {
        setReadPrefTagSets( new ArrayList<String>() );

        String[] parts = tags.split( "#@#" ); //$NON-NLS-1$
        for ( String p : parts ) {
          getReadPrefTagSets().add( p.trim() );
        }
      }
    } catch ( Exception e ) {
      throw new KettleException( BaseMessages.getString( PKG,
              "MongoDbInputMeta.Exception.UnexpectedErrorWhileReadingStepInfo" ), e ); //$NON-NLS-1$
    }
  }

  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
          throws KettleException {
    try {

      super.saveRep( rep, metaStore, id_transformation, id_step );

      rep.saveStepAttribute( id_transformation, id_step, "fields_name", fields ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "json_field_name", jsonFieldName ); //$NON-NLS-1$
      rep.saveStepAttribute( id_transformation, id_step, "json_query", jsonQuery ); //$NON-NLS-1$

      rep.saveStepAttribute( id_transformation, id_step, 0, "output_json", //$NON-NLS-1$
              m_outputJson );
      rep.saveStepAttribute( id_transformation, id_step, 0, "query_is_pipeline", //$NON-NLS-1$
              m_aggPipeline );
      rep.saveStepAttribute( id_transformation, id_step, 0, "allow_disk_use", //$NON-NLS-1$
        allowDiskUse );
      rep.saveStepAttribute( id_transformation, id_step, 0, "execute_for_each_row", //$NON-NLS-1$
              m_executeForEachIncomingRow );

      if ( m_fields != null && m_fields.size() > 0 ) {
        for ( int i = 0; i < m_fields.size(); i++ ) {
          MongoField f = m_fields.get( i );

          rep.saveStepAttribute( id_transformation, id_step, i, "field_name", //$NON-NLS-1$
                  f.m_fieldName );
          rep.saveStepAttribute( id_transformation, id_step, i, "field_path", //$NON-NLS-1$
                  f.m_fieldPath );
          rep.saveStepAttribute( id_transformation, id_step, i, "field_type", //$NON-NLS-1$
                  f.m_kettleType );
          if ( f.m_indexedVals != null && f.m_indexedVals.size() > 0 ) {
            String indexedVals = MongoDbInputData.indexedValsList( f.m_indexedVals );

            rep.saveStepAttribute( id_transformation, id_step, i, "indexed_vals", indexedVals ); //$NON-NLS-1$
          }
        }
      }

      String tags = tagSetsToString();
      if ( !Utils.isEmpty( tags ) ) {
        rep.saveStepAttribute( id_transformation, id_step, "tag_sets", tags ); //$NON-NLS-1$
      }
    } catch ( KettleException e ) {
      throw new KettleException(
              BaseMessages.getString( PKG, "MongoDbInputMeta.Exception.UnableToSaveStepInfo" ) + id_step, e ); //$NON-NLS-1$
    }
  }

  @Override
  public StepInterface getStep( StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta tr,
                                Trans trans ) {
    return new MongoDbInput( stepMeta, stepDataInterface, cnr, tr, trans );
  }

  @Override
  public StepDataInterface getStepData() {
    return new MongoDbInputData();
  }

  /**
   * @return the fields
   */
  public String getFieldsName() {
    return fields;
  }

  /**
   * @param fields a field name to set
   */
  public void setFieldsName( String fields ) {
    this.fields = fields;
  }

  /**
   * @return the jsonFieldName
   */
  public String getJsonFieldName() {
    return jsonFieldName;
  }

  /**
   * @param jsonFieldName
   *          the jsonFieldName to set
   */
  public void setJsonFieldName( String jsonFieldName ) {
    this.jsonFieldName = jsonFieldName;
  }

  /**
   * @return the jsonQuery
   */
  public String getJsonQuery() {
    return jsonQuery;
  }

  /**
   * @param jsonQuery
   *          the jsonQuery to set
   */
  public void setJsonQuery( String jsonQuery ) {
    this.jsonQuery = jsonQuery;
  }

  /**
   * Set whether to output just a single field as JSON
   *
   * @param outputJson
   *          true if a single field containing JSON is to be output
   */
  public void setOutputJson( boolean outputJson ) {
    m_outputJson = outputJson;
  }

  /**
   * Get whether to output just a single field as JSON
   *
   * @return true if a single field containing JSON is to be output
   */
  public boolean getOutputJson() {
    return m_outputJson;
  }

  /**
   * Set whether the supplied query is actually a pipeline specification
   *
   * @param q
   *          true if the supplied query is a pipeline specification
   */
  public void setQueryIsPipeline( boolean q ) {
    m_aggPipeline = q;
  }

  /**
   * Get whether the supplied query is actually a pipeline specification
   *
   * @true true if the supplied query is a pipeline specification
   */
  public boolean getQueryIsPipeline() {
    return m_aggPipeline;
  }

  /**
   * Get whether the supplied query is actually a pipeline specification
   * <p>
   * true if the supplied query is a pipeline specification and allowdiskuse is enabled
   */
  public boolean isAllowDiskUse() {
    return allowDiskUse;
  }

  /**
   * @param allowDiskUse - true if the supplied query is a pipeline specification and allowdiskuse is enabled
   */
  public void setAllowDiskUse( boolean allowDiskUse ) {
    this.allowDiskUse = allowDiskUse;
  }
  
  public MongoCursor<Document> getMongoCursor( MongoCollection<Document> collection, VariableSpace vars, Integer limit,
      Function<String, String> queryModifier, Function<String, String> fieldsProjectionModifier ) throws KettleException {
    
    String query = vars.environmentSubstitute( getJsonQuery() );
    String fields = vars.environmentSubstitute( getFieldsName() );

    // Executing for each row, perform substitutions
    if ( queryModifier != null ) {
      query = queryModifier.apply( query );
    }
    
    if( fieldsProjectionModifier != null ) {
      fields = fieldsProjectionModifier.apply( fields );
    }

    logDetailed( BaseMessages.getString( PKG, "MongoDbInput.Message.ExecutingQuery", query ) );

    if ( getQueryIsPipeline() ) {

      // pipeline aggregations require a query
      if ( Utils.isEmpty( query ) ) {
        throw new KettleException( BaseMessages
            .getString( MongoDbInputMeta.PKG, "MongoDbInput.ErrorMessage.EmptyAggregationPipeline" ) );
      }

      //Make sure the query has array brackets at the front and end
      query = query.trim();
      if( !query.startsWith( "[" ) && !query.endsWith( "]" ) ) {
        query = "[" + query + "]";
      }
      
      BsonArray parsedQuery = BsonArray.parse( query );
      
      if( limit != null && limit > 0 ) {
        parsedQuery.add( BsonDocument.parse( "{ $limit : " + limit + "}" ) );
      }
      
      return collection.aggregate( parsedQuery.stream()
              .map( BsonValue::asDocument )
              .map( BsonDocument::toJson )
              .map( Document::parse )
              .collect( Collectors.toList() ) )
              .allowDiskUse( isAllowDiskUse() )
              .cursor();

    } else {

      FindIterable<Document> findCommand = collection.find();
      if ( !Utils.isEmpty( query ) ) {
        findCommand = findCommand.filter( BasicDBObject.parse( query ) );
      }
      if ( !Utils.isEmpty( fields ) ) {
        findCommand = findCommand.projection( BasicDBObject.parse( fields ) );
      }
      if( limit != null && limit > 0 ) {
        findCommand = findCommand.limit( limit );
      }
      
      return findCommand.iterator();
    }
  }
  
}
