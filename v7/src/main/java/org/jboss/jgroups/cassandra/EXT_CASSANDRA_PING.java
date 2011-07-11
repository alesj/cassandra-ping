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

package org.jboss.jgroups.cassandra;

import org.jboss.jgroups.cassandra.plugins.BaseCassandraSPI;
import org.jboss.jgroups.cassandra.spi.CassandraSPI;
import org.jgroups.Event;
import org.jgroups.annotations.Experimental;

/**
 * Extended Cassandra ping implementation,
 * it uses SPI to create database elements needed for the ping to work.
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
@Experimental
public class EXT_CASSANDRA_PING extends CASSANDRA_PING
{
   private transient CassandraSPI cassandraSPI;

   public Object down(Event event)
   {
      switch (event.getType())
      {
         case Event.CONNECT:
         case Event.CONNECT_WITH_STATE_TRANSFER:
         case Event.CONNECT_USE_FLUSH:
         case Event.CONNECT_WITH_STATE_TRANSFER_USE_FLUSH:
         {
            String clusterName = (String) event.getArg();
            getCassandraSPI().createColumnFamily(keyspace, clusterName);
         }
      }
      try
      {
         return super.down(event);
      }
      finally
      {
         switch (event.getType())
         {
            case Event.DISCONNECT:
            {
               String clusterName = (String) event.getArg();
               getCassandraSPI().dropColumnFamily(keyspace, clusterName);
            }
         }
      }
   }

   public CassandraSPI getCassandraSPI()
   {
      if (cassandraSPI == null)
         cassandraSPI = new BaseCassandraSPI();
      return cassandraSPI;
   }

   public void setCassandraSPI(CassandraSPI cassandraSPI)
   {
      this.cassandraSPI = cassandraSPI;
   }
}
