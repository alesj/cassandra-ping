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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
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
import org.jgroups.util.Streamable;

/**
 * Simple discovery protocol which uses a Apache Cassandra DB. The local
 * address information, e.g. UUID and physical addresses mappings are written to the DB and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/CASSANDRA_PING.txt<p/>
 * <p/>
 * todo: READ below
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
         tr.close();
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
         byte[] id = toBytes(data.getAddress()); // address as unique id?
         client.insert(ByteBuffer.wrap(id), table, new Column(ByteBuffer.wrap(DATA), ByteBuffer.wrap(toBytes(data)), timestamp), ConsistencyLevel.ONE);
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
      // TODO -- find / query all
      return results;
   }

   @Override
   protected void remove(String clustername, Address addr)
   {
      try
      {
         ColumnPath path = new ColumnPath(clustername);
         long timestamp = System.currentTimeMillis();
         client.remove(ByteBuffer.wrap(toBytes(addr)), path, timestamp, ConsistencyLevel.ONE);
      }
      catch (Exception e)
      {
         log.debug("Cannot remove ping data.", e);
      }
   }

   /**
    * Get bytes from PingData.
    *
    * @param data the ping data
    * @return ping data's bytes
    */
   protected byte[] toBytes(Streamable data)
   {
      try
      {
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         data.writeTo(new DataOutputStream(baos));
         return baos.toByteArray();
      }
      catch (IOException e)
      {
         throw new IllegalArgumentException(e);
      }
   }

   /**
    * Get ping data from bytes.
    *
    * @param bytes the bytes
    * @param clazz the expected class
    * @return read ping data
    */
   protected <T extends Streamable> T fromBytes(byte[] bytes, Class<T> clazz)
   {
      try
      {
         ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
         T data = clazz.newInstance();
         data.readFrom(new DataInputStream(bais));
         return data;
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException(e);
      }
   }
}
