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

import static org.jgroups.util.Util.streamableToByteBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.jgroups.Address;
import org.jgroups.annotations.Experimental;
import org.jgroups.annotations.Property;
import org.jgroups.protocols.FILE_PING;
import org.jgroups.protocols.PingData;
import org.jgroups.util.Util;

/**
 * Simple discovery protocol which uses a Apache Cassandra DB. The local
 * address information, e.g. UUID and physical addresses mappings are written to the DB and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/CASSANDRA_PING.txt<p/>
 * <p/>
 * A possible mapping to Cassandra could be to take the clustername-Address (where Address is a UUID), e.g.
 * "MyCluster-15524-355142-335-1dd3" and associate the logical name and physical address with this key as value.<p/>
 * If Cassandra provides search, then to find all nodes in cluster "MyCluster", we'd have to grab all keys starting
 * with "MyCluster". If search is not provided, then this is not good as it requires a linear iteration of all keys...
 * <p/>
 * As an alternative, maybe a Cassandra table can be created named the same as the cluster (e.g. "MyCluster"). Then the
 * keys would be the addresses (UUIDs)
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 * @author Bela Ban
 * @author Matej Lazar
 */
@Experimental
public class CASSANDRA_PING extends FILE_PING
{
   public static final String UTF8 = "UTF8";
   public static final byte[] DATA;

   static
   {
      try
      {
         DATA = "data".getBytes(UTF8);
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException(e);
      }
   }

   @Property(description = "Cassandra host")
   private String host = "localhost";

   @Property(description = "Cassandra port")
   private int port = 9160; // default?

   @Property(description = "Cassandra keyspace")
   private String keyspace = "jgroups";

   private TTransport tr;
   private Cassandra.Client client;

   public CASSANDRA_PING()
   {
      setId((short) 1001); // id OK?
   }

   @Override
   protected void createRootDir()
   {
      try
      {
         tr = new TSocket(host, port);  //new default in 0.7 is framed transport
         TFramedTransport tf = new TFramedTransport(tr);
         TProtocol proto = new TBinaryProtocol(tf);
         client = new Cassandra.Client(proto);
         tf.open();
         client.set_keyspace(keyspace);
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException(e);
      }
   }

   @Override
   public void destroy()
   {
      try
      {
         client = null;
         TTransport temp = tr;
         tr = null;
         temp.close();
      }
      finally
      {
         super.destroy();
      }
   }

   @Override
   protected void writeToFile(PingData data, String clustername)
   {
      try
      {
         ColumnParent table = new ColumnParent(clustername);
         long timestamp = System.currentTimeMillis();
         byte[] id = streamableToByteBuffer(data.getAddress());
         client.insert(ByteBuffer.wrap(id), table, new Column(ByteBuffer.wrap(DATA), ByteBuffer.wrap(streamableToByteBuffer(data)), timestamp), ConsistencyLevel.ONE);
      }
      catch (Exception e)
      {
         log.debug("Cannot write ping data.", e);
      }
   }

   @Override
   protected List<PingData> readAll(String clustername)
   {
      List<PingData> results = new ArrayList<PingData>();
      try
      {
         ColumnParent cp = new ColumnParent(clustername);
         SlicePredicate predicate = new SlicePredicate();
         predicate.setColumn_names(Collections.singletonList(ByteBuffer.wrap(DATA)));
         KeyRange range = new KeyRange();
         range.setStart_key(new byte[0]);
         range.setEnd_key(new byte[0]);
         List<KeySlice> slices = client.get_range_slices(cp, predicate, range, ConsistencyLevel.ONE);
         for (KeySlice ks : slices)
         {
            List<ColumnOrSuperColumn> columns = ks.getColumns();
            if (columns.isEmpty())
               continue;

            ColumnOrSuperColumn column = columns.get(0);
            byte[] bytes = column.column.getValue();
            PingData data = (PingData) Util.streamableFromByteBuffer(PingData.class, bytes);
            results.add(data);
         }
         return results;
      }
      catch (Exception e)
      {
         log.debug(e.getMessage());
      }
      return results;
   }

   @Override
   protected void remove(String clustername, Address addr)
   {
      try
      {
         ColumnPath path = new ColumnPath(clustername);
         long timestamp = System.currentTimeMillis();
         client.remove(ByteBuffer.wrap(streamableToByteBuffer(addr)), path, timestamp, ConsistencyLevel.ONE);
      }
      catch (Exception e)
      {
         log.debug("Cannot remove ping data.", e);
      }
   }
}
