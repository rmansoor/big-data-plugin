/*******************************************************************************
 * Pentaho Big Data
 * <p>
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 * <p>
 * ******************************************************************************
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 ******************************************************************************/
package org.pentaho.big.data.impl.vfs.hdfs.nc;

import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.FileNameParser;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.pentaho.big.data.api.cluster.NamedCluster;
import org.pentaho.big.data.api.cluster.NamedClusterListener;
import org.pentaho.big.data.api.cluster.NamedClusterService;
import org.pentaho.big.data.api.initializer.ClusterInitializationException;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileProvider;
import org.pentaho.big.data.impl.vfs.hdfs.HDFSFileSystem;
import org.pentaho.bigdata.api.hdfs.HadoopFileSystemLocator;
import org.pentaho.di.core.variables.Variables;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.metastore.api.IMetaStore;
import org.pentaho.metastore.api.exceptions.MetaStoreException;
import org.pentaho.osgi.metastore.locator.api.MetastoreLocator;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by dstepanov on 11/05/17.
 */
public class NamedClusterProvider extends HDFSFileProvider implements NamedClusterListener {

  private MetastoreLocator metaStoreService;

  private Set<String> namedClusterDeletedList = new HashSet<String>();

  private Set<String> namedClusterUpdatedList = new HashSet<String>();

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               FileNameParser fileNameParser,
                               String[] schemes,
                               MetastoreLocator metaStore ) throws FileSystemException {
    this(
      hadoopFileSystemLocator,
      namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
      fileNameParser,
      schemes,
      metaStore );
  }

  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               FileNameParser fileNameParser,
                               String schema,
                               MetastoreLocator metaStore ) throws FileSystemException {
    this(
      hadoopFileSystemLocator,
      namedClusterService,
      (DefaultFileSystemManager) KettleVFS.getInstance().getFileSystemManager(),
      fileNameParser,
      new String[] { schema },
      metaStore );
  }


  public NamedClusterProvider( HadoopFileSystemLocator hadoopFileSystemLocator,
                               NamedClusterService namedClusterService,
                               DefaultFileSystemManager fileSystemManager,
                               FileNameParser fileNameParser,
                               String[] schemes,
                               MetastoreLocator metaStore ) throws FileSystemException {
    super( hadoopFileSystemLocator, namedClusterService, fileSystemManager, fileNameParser, schemes );
    this.metaStoreService = metaStore;
    namedClusterService.addNamedClusterListener( this );
  }


  @Override
  protected FileSystem doCreateFileSystem( FileName name, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    GenericFileName genericFileName = (GenericFileName) name.getRoot();
    String clusterName = genericFileName.getHostName();
    String path = genericFileName.getPath();
    NamedCluster namedCluster = getNamedClusterByName( clusterName );
    try {
      if ( namedCluster == null ) {
        namedCluster = namedClusterService.getClusterTemplate();
      }
      String generatedUrl = namedCluster
        .processURLsubstitution( path == null ? "" : path, metaStoreService.getMetastore(), new Variables() );
      URI uri = URI.create( generatedUrl );

      return new HDFSFileSystem( name, fileSystemOptions,
        hadoopFileSystemLocator.getHadoopFilesystem( namedCluster, uri ) );
    } catch ( ClusterInitializationException e ) {
      throw new FileSystemException( e );
    }
  }


  @Override
  public FileSystemConfigBuilder getConfigBuilder() {
    return NamedClusterConfigBuilder.getInstance( metaStoreService, namedClusterService );
  }

  private NamedCluster getNamedClusterByName( String clusterNameToResolve ) throws FileSystemException {
    IMetaStore metaStore = metaStoreService.getMetastore();
    NamedCluster namedCluster = null;
    try {
      if ( metaStore != null ) {
        namedCluster = namedClusterService.read( clusterNameToResolve, metaStore );
      }
    } catch ( MetaStoreException e ) {
      throw new FileSystemException( e );
    }
    return namedCluster;
  }

  @Override
  protected synchronized FileSystem getFileSystem( FileName rootName, FileSystemOptions fileSystemOptions )
    throws FileSystemException {
    // Find the cluster name from the list of named cluster that were deleted
    String namedClusterDeleted = findNamedCluster( namedClusterDeletedList , rootName.getURI());
    // If we have found a NamedCluster that exists in the rootName we will close the file system and return null
    if ( namedClusterDeleted != null && namedClusterDeleted.length() > 0 ) {
      // Get the old file system from the cache
      FileSystem existingFs = findFileSystem( rootName, fileSystemOptions );
      // Remove the file system from the cache
      closeFileSystem( existingFs );
      // Remove the cluster name from the list of clusters that were removed
      namedClusterDeletedList.remove( namedClusterDeleted );
      return null;
    }

      // Find the cluster name in the list of named clusters that were updated
    String namedClusterChanged = findNamedCluster( namedClusterUpdatedList , rootName.getURI());
    // If we have found a NamedCluster that exists in the rootName close the existing file system and create a new one
    if ( namedClusterChanged != null && namedClusterChanged.length() > 0 ) {
        // Get the old file system from the cache
        FileSystem existingFs = findFileSystem( rootName, fileSystemOptions );
        // Remove the file system from the cache
        closeFileSystem( existingFs );
        // create a new file system based on the updated NamedCluster
        FileSystem newFs = doCreateFileSystem( rootName, fileSystemOptions );
        // Add this new file system to the cache
        addFileSystem( rootName, newFs );
        // Remove the cluster name from the list of clusters that were changed
        namedClusterUpdatedList.remove( namedClusterChanged );
        return newFs;
    } else {
      // The name cluster has not changed to use the ond in the cache.
      FileSystem fs = findFileSystem(rootName, fileSystemOptions);
      if (fs == null)
      {
        // Need to create the file system, and cache it
        fs = doCreateFileSystem(rootName, fileSystemOptions);
        // Add this new file system to the cache
        addFileSystem(rootName, fs);
      }
      return fs;
    }
  }

  @Override
  public void onDelete( NamedCluster namedCluster ) {
    namedClusterDeletedList.add( namedCluster.getName() );
    String nc = findNamedCluster( namedClusterUpdatedList, namedCluster.getName() );
    if( nc != null && nc.length() > 0 ) {
      namedClusterUpdatedList.remove( nc );
    }

  }

  @Override
  public void onUpdate( NamedCluster namedCluster ) {
    namedClusterUpdatedList.add( namedCluster.getName() );
    String nc = findNamedCluster( namedClusterDeletedList, namedCluster.getName() );
    if( nc != null && nc.length() > 0 ) {
      namedClusterDeletedList.remove( nc );
    }
  }

  /**
   * Find the cluster name in the list of cluster name that has changed or removed
   * @param set
   * @param name
   * @return cluster name
   */
  private String findNamedCluster(Set set, String name ) {
    for ( Object item : set ) {
      String namedClusterName = null;
      if ( item instanceof String) {
        namedClusterName = (String) item;
        if ( name != null && name.contains( namedClusterName ) ) {
          return namedClusterName;
        }
      }
    }
    return null;
  }

}
