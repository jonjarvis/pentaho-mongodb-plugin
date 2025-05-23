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


package org.pentaho.di.trans.steps.mongodb;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.encryption.Encr;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.util.Utils;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public abstract class MongoDbMeta extends BaseStepMeta implements StepMetaInterface {
  protected static Class<?> PKG = MongoDbInputMeta.class; // for i18n purposes

  public static final int DEFAULT_MONGODB_PORT = 27017;
  
  @Injection( name = "HOSTNAME" )
  private String hostname = "localhost";

  @Injection( name = "PORT" )
  private String port = Integer.toString( DEFAULT_MONGODB_PORT );

  @Injection( name = "DATABASE_NAME" )
  private String dbName;
  
  @Injection( name = "COLLECTION" )
  private String collection;

  @Injection( name = "AUTH_DATABASE" )
  private String authenticationDatabaseName;
  
  @Injection( name = "AUTH_USERNAME" )
  private String authenticationUser;
  
  @Injection( name = "AUTH_PASSWORD" )
  private String authenticationPassword;

  @Injection( name = "AUTH_MECHANISM" )
  private String authenticationMechanism = "";
  
  @Injection( name = "AUTH_KERBEROS" )
  private boolean m_kerberos;

  @Injection( name = "TIMEOUT_CONNECTION" )
  private String m_connectTimeout = ""; // default - never time out //$NON-NLS-1$

  @Injection( name = "TIMEOUT_SOCKET" )
  private String m_socketTimeout = ""; // default - never time out //$NON-NLS-1$

  /**
   * primary, primaryPreferred, secondary, secondaryPreferred, nearest
   */
  @Injection( name = "READ_PREFERENCE" )
  private String m_readPreference = "PRIMARY"; //NamedReadPreference.PRIMARY.getName();

  /**
   * whether to discover and use all replica set members (if not already
   * specified in the hosts field)
   */
  @Injection( name = "USE_ALL_REPLICA_SET_MEMBERS" )
  private boolean m_useAllReplicaSetMembers;

  /**
   * optional tag sets to use with read preference settings
   */
  @Injection( name = "TAG_SET" )
  private List<String> m_readPrefTagSets;

  @Injection( name = "USE_SSL_SOCKET_FACTORY" )
  private boolean m_useSSLSocketFactory;

  /**
   * parameter to differentiate between ConnectionString URI connections and normal connections
   */
  @Injection( name = "USE_CONNECTION_STRING" )
  private boolean m_useConnectionString;

  /**
   * parameter to differentiate between ConnectionString URI connections and normal connections
   */
  @Injection( name = "USE_LEGACY_OPTIONS" )
  private boolean m_useLegacyOptions;

  /**
   * ConnectionString URI parameter for ConnectionString URI connections
   */
  @Injection( name = "CONNECTION_STRING" )
  private String connectionString;


  /**
   * default = 1 (standalone or primary acknowledges writes; -1 no
   * acknowledgement and all errors suppressed; 0 no acknowledgement, but
   * socket/network errors passed to client; "majority" returns after a majority
   * of the replica set members have acknowledged; n (>1) returns after n
   * replica set members have acknowledged; tags (string) specific replica set
   * members with the tags need to acknowledge
   */
  private String m_writeConcern = ""; //$NON-NLS-1$

  /**
   * The time in milliseconds to wait for replication to succeed, as specified
   * in the w option, before timing out
   */
  private String m_wTimeout = ""; //$NON-NLS-1$

  /**
   * whether write operations will wait till the mongod acknowledges the write
   * operations and commits the data to the on disk journal
   */
  private boolean m_journal;

  public void setReadPrefTagSets( List<String> tagSets ) {
    m_readPrefTagSets = tagSets;
  }

  public List<String> getReadPrefTagSets() {
    return m_readPrefTagSets;
  }

  public void setUseAllReplicaSetMembers( boolean u ) {
    m_useAllReplicaSetMembers = u;
  }

  public boolean getUseAllReplicaSetMembers() {
    return m_useAllReplicaSetMembers;
  }

  /**
   * @return the hostnames (comma separated: host:<port>)
   */
  public String getHostnames() {
    return hostname;
  }

  /**
   * @param hostname the hostnames to set (comma separated: host:<port>)
   */
  public void setHostnames( String hostname ) {
    this.hostname = hostname;
  }

  /**
   * @return the port. This is a port to use for all hostnames (avoids having to
   * specify the same port for each hostname in the hostnames list
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port. This is a port to use for all hostnames (avoids
   *             having to specify the same port for each hostname in the hostnames
   *             list
   */
  public void setPort( String port ) {
    this.port = port;
  }

  /**
   * @return the dbName
   */
  public String getDbName() {
    return dbName;
  }

  /**
   * @param dbName the dbName to set
   */
  public void setDbName( String dbName ) {
    this.dbName = dbName;
  }

  /**
   * @return the collection
   */
  public String getCollection() {
    return collection;
  }

  /**
   * @param collection the collection to set
   */
  public void setCollection( String collection ) {
    this.collection = collection;
  }

  /**
   * Get the AuthenticationDatabase parameter.
   *
   * @return an authentication database.
   */
  public String getAuthenticationDatabaseName() {
    return authenticationDatabaseName;
  }

  /**
   * Set the AuthenticationDatabase parameter.
   *
   * @param authenticationDatabaseName an authentication database to set.
   */
  public void setAuthenticationDatabaseName( String authenticationDatabaseName ) {
    this.authenticationDatabaseName = authenticationDatabaseName;
  }

  /**
   * @return the authenticationUser
   */
  public String getAuthenticationUser() {
    return authenticationUser;
  }

  /**
   * @param authenticationUser the authenticationUser to set
   */
  public void setAuthenticationUser( String authenticationUser ) {
    this.authenticationUser = authenticationUser;
  }

  /**
   * @return the authenticationPassword
   */
  public String getAuthenticationPassword() {
    return authenticationPassword;
  }

  /**
   * @param authenticationPassword the authenticationPassword to set
   */
  public void setAuthenticationPassword( String authenticationPassword ) {
    this.authenticationPassword = authenticationPassword;
  }

  /**
   * Set whether to use kerberos authentication
   *
   * @param k true if kerberos is to be used
   */
  public void setUseKerberosAuthentication( boolean k ) {
    m_kerberos = k;
  }

  /**
   * Get whether to use kerberos authentication
   *
   * @return true if kerberos is to be used
   */
  public boolean getUseKerberosAuthentication() {
    return m_kerberos;
  }

  /**
   * Set the connection timeout. The default is never timeout
   *
   * @param to the connection timeout in milliseconds
   */
  public void setConnectTimeout( String to ) {
    m_connectTimeout = to;
  }

  /**
   * Get the connection timeout. The default is never timeout
   *
   * @return the connection timeout in milliseconds
   */
  public String getConnectTimeout() {
    return m_connectTimeout;
  }

  /**
   * Set the number of milliseconds to attempt a send or receive on a socket
   * before timing out.
   *
   * @param so the number of milliseconds before socket timeout
   */
  public void setSocketTimeout( String so ) {
    m_socketTimeout = so;
  }

  /**
   * Get the number of milliseconds to attempt a send or receive on a socket
   * before timing out.
   *
   * @return the number of milliseconds before socket timeout
   */
  public String getSocketTimeout() {
    return m_socketTimeout;
  }

  /**
   * Set the read preference to use - primary, primaryPreferred, secondary,
   * secondaryPreferred or nearest.
   *
   * @param preference the read preference to use
   */
  public void setReadPreference( String preference ) {
    m_readPreference = preference;
  }

  /**
   * Get the read preference to use - primary, primaryPreferred, secondary,
   * secondaryPreferred or nearest.
   *
   * @return the read preference to use
   */
  public String getReadPreference() {
    return m_readPreference;
  }

  /**
   * Set the write concern to use
   *
   * @param concern the write concern to use
   */
  public void setWriteConcern( String concern ) {
    m_writeConcern = concern;
  }

  /**
   * Get the write concern to use
   *
   * @return  the write concern to use
   */
  public String getWriteConcern() {
    return m_writeConcern;
  }

  /**
   * Set the time in milliseconds to wait for replication to succeed, as
   * specified in the w option, before timing out
   *
   * @param w the timeout to use
   */
  public void setWTimeout( String w ) {
    m_wTimeout = w;
  }

  /**
   * Get the time in milliseconds to wait for replication to succeed, as
   * specified in the w option, before timing out
   *
   * @return the timeout to use
   */
  public String getWTimeout() {
    return m_wTimeout;
  }

  /**
   * Set whether to use journaled writes
   *
   * @param j true for journaled writes
   */
  public void setJournal( boolean j ) {
    m_journal = j;
  }

  /**
   * Get whether to use journaled writes
   *
   * @return true for journaled writes
   */
  public boolean getJournal() {
    return m_journal;
  }

  /**
   * Get Mongo authentication mechanism
   *
   */
  public String getAuthenticationMechanism() {
    return authenticationMechanism;
  }
  /**
   * Set Mongo authentication mechanism
   *
   */
  public void setAuthenticationMechanism( String authenticationMechanism ) {
    this.authenticationMechanism = authenticationMechanism;
  }

  public boolean isUseSSLSocketFactory() {
    return m_useSSLSocketFactory;
  }

  public void setUseSSLSocketFactory( boolean value ) {
    this.m_useSSLSocketFactory = value;
  }

  /**
   * Get whether Connection String URI mechanism is used
   *
   * @return true for connection String URI mechanism
   */
  public boolean isUseConnectionString() {
    return m_useConnectionString;
  }

  /**
   * Set whether to use true for connection String URI mechanism
   *
   * @param m_useConnectionString true for connection String URI mechanism
   */
  public void setUseConnectionString( boolean m_useConnectionString ) {
    this.m_useConnectionString = m_useConnectionString;
  }

  /**
   * Get whether Legacy Options (specifying hosts, port information individually) mechanism is used
   *
   * @return true for Legacy options mechanism
   */
  public boolean isUseLegacyOptions() {
    return m_useLegacyOptions;
  }

  /**
   * Set whether to use Legacy Options (specifying hosts, port information individually) mechanism is used
   *
   * @param m_useLegacyOptions true for Legacy options mechanism
   */

  public void setUseLegacyOptions( boolean m_useLegacyOptions ) {
    this.m_useLegacyOptions = m_useLegacyOptions;
  }

  /**
   * Get connection string
   *
   * @return connection sting
   */

  public String getConnectionString() {
    return connectionString;
  }

  /**
   * Set connection string
   *
   * @param connectionString string used to connect
   */

  public void setConnectionString( String connectionString ) {
    this.connectionString = connectionString;
  }
  
  @Override
  public void readRep( Repository rep, IMetaStore metaStore, ObjectId id_step, List<DatabaseMeta> databases )
    throws KettleException {
    
    setUseConnectionString( rep.getStepAttributeBoolean( id_step, 0, "use_connection_string" ) );
    setUseLegacyOptions( rep.getStepAttributeBoolean( id_step, 0, "use_legacy_options" ) );
    setConnectionString( Encr.decryptPasswordOptionallyEncrypted(
            rep.getStepAttributeString( id_step, "connection_string" ) ) );
    setHostnames( rep.getStepAttributeString( id_step, "hostname" ) ); //$NON-NLS-1$
    setPort( rep.getStepAttributeString( id_step, "port" ) ); //$NON-NLS-1$
    setUseAllReplicaSetMembers( rep.getStepAttributeBoolean( id_step, 0, "use_all_replica_members" ) ); //$NON-NLS-1$
    setDbName( rep.getStepAttributeString( id_step, "db_name" ) ); //$NON-NLS-1$
    
    setCollection( rep.getStepAttributeString( id_step, "collection" ) ); //$NON-NLS-1$

    setAuthenticationDatabaseName( rep.getStepAttributeString( id_step, "auth_database" ) ); //$NON-NLS-1$
    setAuthenticationMechanism( rep.getStepAttributeString( id_step, "auth_mech" ) );
    setAuthenticationUser( rep.getStepAttributeString( id_step, "auth_user" ) ); //$NON-NLS-1$
    setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( rep.getStepAttributeString( id_step,
            "auth_password" ) ) ); //$NON-NLS-1$
    setUseKerberosAuthentication( rep.getStepAttributeBoolean( id_step, "auth_kerberos" ) ); //$NON-NLS-1$
    setConnectTimeout( rep.getStepAttributeString( id_step, "connect_timeout" ) ); //$NON-NLS-1$
    setSocketTimeout( rep.getStepAttributeString( id_step, "socket_timeout" ) ); //$NON-NLS-1$
    setUseSSLSocketFactory( rep.getStepAttributeBoolean( id_step, 0, "use_ssl_socket_factory", false ) );
    setReadPreference( rep.getStepAttributeString( id_step, "read_preference" ) ); //$NON-NLS-1$
    
  }
  
  @Override
  public void saveRep( Repository rep, IMetaStore metaStore, ObjectId id_transformation, ObjectId id_step )
    throws KettleException {
    
    rep.saveStepAttribute( id_transformation, id_step, "use_connection_string", isUseConnectionString() );
    rep.saveStepAttribute( id_transformation, id_step, "use_legacy_options", isUseLegacyOptions() );
    rep.saveStepAttribute( id_transformation, id_step, "connection_string",
            Encr.encryptPasswordIfNotUsingVariables( getConnectionString() ) );
    rep.saveStepAttribute( id_transformation, id_step, "hostname", getHostnames() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "port", getPort() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "use_all_replica_members", getUseAllReplicaSetMembers() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "db_name", getDbName() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "collection", getCollection() ); //$NON-NLS-1$
    
    rep.saveStepAttribute( id_transformation, id_step, "auth_database", //$NON-NLS-1$
            getAuthenticationDatabaseName() );
    rep.saveStepAttribute( id_transformation, id_step, "auth_user", //$NON-NLS-1$
            getAuthenticationUser() );
    rep.saveStepAttribute( id_transformation, id_step, "auth_password", //$NON-NLS-1$
            Encr.encryptPasswordIfNotUsingVariables( getAuthenticationPassword() ) );
    rep.saveStepAttribute( id_transformation, id_step, "auth_mech", getAuthenticationMechanism() );
    rep.saveStepAttribute( id_transformation, id_step, "auth_kerberos", getUseKerberosAuthentication() );
    rep.saveStepAttribute( id_transformation, id_step, "connect_timeout", getConnectTimeout() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "socket_timeout", getSocketTimeout() ); //$NON-NLS-1$
    rep.saveStepAttribute( id_transformation, id_step, "use_ssl_socket_factory", isUseSSLSocketFactory() );
    rep.saveStepAttribute( id_transformation, id_step, "read_preference", getReadPreference() ); //$NON-NLS-1$
    
  }
  
  @Override
  public void loadXML( Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore ) throws KettleXMLException {
    
    String useConnectionString =  XMLHandler.getTagValue( stepnode, "use_connection_string" );
    String useLegacyOptions =  XMLHandler.getTagValue( stepnode, "use_legacy_options" );
    if ( !Utils.isEmpty( useConnectionString ) && useConnectionString.equalsIgnoreCase( "Y" ) ) {
      setUseConnectionString( true );
    } else if ( !Utils.isEmpty( useLegacyOptions ) && useLegacyOptions.equalsIgnoreCase( "Y" ) ) {
      setUseLegacyOptions( true );
    } else {
      setUseLegacyOptions( true );
    }
    setConnectionString( Encr.decryptPasswordOptionallyEncrypted(
            XMLHandler.getTagValue( stepnode, "connection_string" ) ) );
    setHostnames( XMLHandler.getTagValue( stepnode, "hostname" ) ); //$NON-NLS-1$
    setPort( XMLHandler.getTagValue( stepnode, "port" ) ); //$NON-NLS-1$
    setDbName( XMLHandler.getTagValue( stepnode, "db_name" ) ); //$NON-NLS-1$
    setCollection( XMLHandler.getTagValue( stepnode, "collection" ) ); //$NON-NLS-1$

    setAuthenticationDatabaseName( XMLHandler.getTagValue( stepnode, "auth_database" ) ); //$NON-NLS-1$
    setAuthenticationUser( XMLHandler.getTagValue( stepnode, "auth_user" ) ); //$NON-NLS-1$
    setAuthenticationPassword( Encr.decryptPasswordOptionallyEncrypted( XMLHandler.getTagValue( stepnode,
      "auth_password" ) ) ); //$NON-NLS-1$

    setAuthenticationMechanism( XMLHandler.getTagValue( stepnode, "auth_mech" ) );
    boolean kerberos = false;
    String useKerberos = XMLHandler.getTagValue( stepnode, "auth_kerberos" ); //$NON-NLS-1$
    if ( !Utils.isEmpty( useKerberos ) ) {
      kerberos = useKerberos.equalsIgnoreCase( "Y" );
    }
    setUseKerberosAuthentication( kerberos );
    setConnectTimeout( XMLHandler.getTagValue( stepnode, "connect_timeout" ) ); //$NON-NLS-1$
    setSocketTimeout( XMLHandler.getTagValue( stepnode, "socket_timeout" ) ); //$NON-NLS-1$
    String useSSLSocketFactory =  XMLHandler.getTagValue( stepnode, "use_ssl_socket_factory" );
    if ( !Utils.isEmpty( useSSLSocketFactory ) ) {
      setUseSSLSocketFactory( useSSLSocketFactory.equalsIgnoreCase( "Y" ) );
    }
    setReadPreference( XMLHandler.getTagValue( stepnode, "read_preference" ) ); //$NON-NLS-1$
    

    
  }
  
  
  public void getXML( StringBuilder retval ) {
    
    retval.append( "    " ).append( XMLHandler.addTagValue( "use_connection_string", isUseConnectionString() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "use_legacy_options", isUseLegacyOptions() ) );
    retval.append( "    " ).append( XMLHandler.addTagValue( "connection_string",
            Encr.encryptPasswordIfNotUsingVariables( getConnectionString() ) ) );
    
    
    retval.append( "    " ).append( XMLHandler.addTagValue( "hostname", getHostnames() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "port", getPort() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "use_all_replica_members", getUseAllReplicaSetMembers() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "db_name", getDbName() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( XMLHandler.addTagValue( "collection", getCollection() ) ); //$NON-NLS-1$ //$NON-NLS-2$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "auth_database", getAuthenticationDatabaseName() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "auth_user", getAuthenticationUser() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "auth_password", //$NON-NLS-1$
                    Encr.encryptPasswordIfNotUsingVariables( getAuthenticationPassword() ) ) );
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "auth_mech", getAuthenticationMechanism() ) );
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "auth_kerberos", getUseKerberosAuthentication() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "connect_timeout", getConnectTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "socket_timeout", getSocketTimeout() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "use_ssl_socket_factory", isUseSSLSocketFactory() ) ); //$NON-NLS-1$
    retval.append( "    " ).append( //$NON-NLS-1$
            XMLHandler.addTagValue( "read_preference", getReadPreference() ) ); //$NON-NLS-1$
    
  }
  
  
  protected MongoClientSettings getMongoClientSettings( VariableSpace vs ) throws KettleException {
  
    MongoClientSettings.Builder builder = MongoClientSettings.builder();
    
    // Apply Host(s) & Port Settings --------------------------------------
    String hostSub = vs.environmentSubstitute( getHostnames() );
    if ( Utils.isEmpty( hostSub ) ) {
      throw new KettleException( BaseMessages.getString( PKG, "MongoNoAuthWrapper.Message.Error.EmptyHostsString" ) );
    }
    
    try {   
      
      int singlePort = getVariableInt( getPort(), vs, DEFAULT_MONGODB_PORT ).get();
      
      builder.applyToClusterSettings( b -> b.hosts(
        Arrays.stream( hostSub.split( "," ) ).map( host -> {
          String[] parts = host.split( ":" );
          if ( parts.length > 2 ) {
            throw new RuntimeException( BaseMessages.getString( PKG, "MongoDBMeta.Message.Error.MalformedHost", host ) );
          }
          int portToUse = singlePort;
          if ( parts.length == 2 ) {
            try {
              portToUse = Integer.parseInt( parts[1].trim() );
            } catch ( NumberFormatException nfe ) {
              throw new RuntimeException(
                  BaseMessages.getString( PKG, "MongoDBMeta.Message.Error.UnableToParsePortNumber", parts[1] ), nfe );
            }
          }
          return new ServerAddress( parts[0].trim(), portToUse );
        } ).collect( Collectors.toList() ) ) );
    } catch( Exception e ) {
      throw new KettleException( e );
    }
    
    // Apply SSL Settings ----------------------------------------------------
    
    builder.applyToSslSettings( b -> b.enabled( isUseSSLSocketFactory() ) );
    
    // Apply Timeout Settings ------------------------------------------------

    getVariableInt( getSocketTimeout(), vs, null )
        .ifPresent( st -> builder.applyToSocketSettings( b -> b.readTimeout( st, TimeUnit.MILLISECONDS ) ) );
    getVariableInt( getConnectTimeout(), vs, null )
        .ifPresent( ct -> builder.applyToSocketSettings( b -> b.connectTimeout( ct, TimeUnit.MILLISECONDS ) ) );

    // Apply Credentials -----------------------------------------------------
    
    String userName = vs.environmentSubstitute( this.getAuthenticationUser() );
    if( !Utils.isEmpty( userName ) ) {
      
      if( getUseKerberosAuthentication() ) {
        
        builder.credential( MongoCredential.createGSSAPICredential( userName ) );
        
      } else {
        
        String authDb = Const.NVL( vs.environmentSubstitute( this.getAuthenticationDatabaseName() ), vs.environmentSubstitute( getDbName() ) );
        String password = Encr.decryptPasswordOptionallyEncrypted( vs.environmentSubstitute( Const.NVL(getAuthenticationPassword(), "") ) );
      
        switch( MongoCredentialTypes.valueOfOrDefault( getAuthenticationMechanism(), MongoCredentialTypes.PLAIN ) ) {
          case SCRAMSHA1:
            builder.credential( MongoCredential.createScramSha1Credential( userName, authDb, password.toCharArray() ) );
            break;
          case SCRAMSHA256:
            builder.credential( MongoCredential.createScramSha256Credential( userName, authDb, password.toCharArray() ) );
            break;
          case MONGODBCR:
            builder.credential( MongoCredential.createCredential( userName, authDb, password.toCharArray() ) );
            break;
          case PLAIN:
            builder.credential( MongoCredential.createPlainCredential( userName, authDb, password.toCharArray() ) );
            break;
          default:
            break;
        }
      }
    }
    
    return builder.build();
  }
  

  
  private Optional<Integer> getVariableInt( String value, VariableSpace vs, Integer defaultValue ) {
    try {
      return Optional.ofNullable( Integer.parseInt( vs.environmentSubstitute( value ) ) );
    } catch( Exception e ) {
      return Optional.ofNullable( defaultValue );
    }
  }
  
  
  public MongoClient getMongoClient( VariableSpace vs ) throws KettleException {
    
    if( isUseConnectionString() ) {
      return MongoClients.create( Encr.decryptPasswordOptionallyEncrypted( vs.environmentSubstitute( getConnectionString() ) ) );
    } else {
      return MongoClients.create( getMongoClientSettings( vs ) );
    }
   
  }
  
  public static enum MongoCredentialTypes {
    SCRAMSHA1( "SCRAM-SHA-1" ),
    SCRAMSHA256( "SCRAM-SHA256" ),
    MONGODBCR( "MONGODB-CR"),
    PLAIN( "PLAIN" );
    
    private final String authMechanismName;
    
    private MongoCredentialTypes( String mechanismName ) {
      this.authMechanismName = mechanismName;
    }
    
    public String getMechanismName() {
      return authMechanismName;
    }
    
    public static final MongoCredentialTypes valueOfOrDefault( String mechanismName, MongoCredentialTypes def ) {
      
      if( Utils.isEmpty( mechanismName ) ) {
        return def;
      }
      
      for( MongoCredentialTypes type : values() ) {
        if( type.getMechanismName().equalsIgnoreCase( mechanismName ) ) {
          return type;
        }
      }
     
      return def;
    }
    
    
  }
  
  
}
