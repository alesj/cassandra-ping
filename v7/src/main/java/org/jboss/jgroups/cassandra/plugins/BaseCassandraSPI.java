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

package org.jboss.jgroups.cassandra.plugins;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.jboss.jgroups.cassandra.spi.CassandraSPI;

/**
 * Base Cassandra SPI impl.
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class BaseCassandraSPI implements CassandraSPI
{
   private String host = "localhost";
   private int port = 9160; // default?
   private String strategyClass = SimpleStrategy.class.getName();

   protected <T> T execute(ClientExecutor<T> executor)
   {
      TTransport tr = new TSocket(host, port);
      TFramedTransport tf = new TFramedTransport(tr);
      TProtocol proto = new TBinaryProtocol(tf);
      Cassandra.Client client = new Cassandra.Client(proto);
      try
      {
         tf.open();
         return executor.execute(client);
      }
      catch (Throwable t)
      {
         throw new RuntimeException(t);
      }
      finally
      {
         tf.close();
      }
   }

   public boolean createKeyspace(final String keyspaceName)
   {
      return execute(new ClientExecutor<Boolean>()
      {
         public Boolean execute(Cassandra.Client client) throws Throwable
         {
            List<KsDef> ksDefs = client.describe_keyspaces();
            for (KsDef ksDef : ksDefs)
               if (ksDef.getName().equals(keyspaceName))
                  return true;

            KsDef ksDef = new KsDef(keyspaceName, strategyClass, 1, Collections.<CfDef>emptyList());
            client.system_add_keyspace(ksDef);
            return false;
         }
      });
   }

   public void dropKeyspace(final String keyspaceName)
   {
      execute(new ClientExecutor<Object>()
      {
         public Object execute(Cassandra.Client client) throws Throwable
         {
            client.system_drop_keyspace(keyspaceName);
            return null;
         }
      });
   }

   public boolean createColumnFamily(final String keyspaceName, final String columnFamily)
   {
      return execute(new ClientExecutor<Boolean>()
      {
         public Boolean execute(Cassandra.Client client) throws Throwable
         {
            KsDef ksDef = client.describe_keyspace(keyspaceName);
            Iterator<CfDef> iter = ksDef.getCf_defsIterator();
            while (iter.hasNext())
            {
               CfDef cfDef = iter.next();
               if (cfDef.getName().equals(columnFamily))
                  return true;
            }

            client.set_keyspace(keyspaceName);
            CfDef cfDef = new CfDef(keyspaceName, columnFamily);
            client.system_add_column_family(cfDef);
            return false;
         }
      });
   }

   public void dropColumnFamily(final String keyspaceName, final String columnFamily)
   {
      execute(new ClientExecutor<Object>()
      {
         public Object execute(Cassandra.Client client) throws Throwable
         {
            client.set_keyspace(keyspaceName);
            client.system_drop_column_family(columnFamily);
            return null;
         }
      });
   }

   public void setHost(String host)
   {
      this.host = host;
   }

   public void setPort(int port)
   {
      this.port = port;
   }

   public void setStrategyClass(String strategyClass)
   {
      this.strategyClass = strategyClass;
   }
}
