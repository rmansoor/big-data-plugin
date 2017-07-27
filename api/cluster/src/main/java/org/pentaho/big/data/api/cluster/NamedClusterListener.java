package org.pentaho.big.data.api.cluster;

/**
 * Created by rmansoor on 7/26/2017.
 */
public interface NamedClusterListener {

  /**
   * Perform some operation when a cluster is updated
   *
   * @param namedCluster
   */
  public void onUpdate ( NamedCluster namedCluster );

  /**
   * Perform some operation when a cluster is created
   *
   * @param namedCluster
   */
  public void onDelete ( NamedCluster namedCluster );

}
