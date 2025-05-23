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

import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.trans.steps.mongodbinput.MongoDbInputMeta;
import org.pentaho.mongo.wrapper.field.MongoField;

import java.util.List;

/**
 * Created by brendan on 11/4/14.
 */
public interface MongoDbInputDiscoverFields {

  List<MongoField> discoverFields( MongoDbInputMeta step, VariableSpace vars, int docsToSample ) throws KettleException;

}
