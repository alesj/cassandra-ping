/*
* JBoss, Home of Professional Open Source
* Copyright $today.year Red Hat Inc. and/or its affiliates and other
* contributors as indicated by the @author tags. All rights reserved.
* See the copyright.txt in the distribution for a full listing of
* individual contributors.
* 
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
* 
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
* 
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
*/

package org.jboss.jgroups.cassandra.spi;

/**
 * Simple Cassandra SPI; used to simplify communication with low level Cassandra API.
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public interface CassandraSPI
{
   /**
    * Create Cassandra keyspace.
    *
    * @param keyspaceName the keyspace name
    * @return true if new keyspace was created, false otherwise;
    *         e.g. keyspace could exist before, hence we return false
    */
   boolean createKeyspace(String keyspaceName);

   /**
    * Drop Cassandra keyspace.
    *
    * @param keyspaceName the keyspace name
    */
   void dropKeyspace(String keyspaceName);

   /**
    * Create Cassandra column family.
    *
    * @param keyspaceName the keyspace name
    * @param columnFamily the column family
    * @return true if new column family was created, false otherwise;
    *         e.g. column family could exist before, hence we return false
    */
   boolean createColumnFamily(String keyspaceName, String columnFamily);

   /**
    * Drop Cassandra column family.
    *
    * @param keyspaceName the keyspace name
    * @param columnFamily the column family
    */
   void dropColumnFamily(String keyspaceName, String columnFamily);
}
