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

package org.jboss.test.jgroups.cassandra.support;

import java.util.List;

import org.jboss.jgroups.cassandra.CASSANDRA_PING;
import org.jgroups.Address;
import org.jgroups.protocols.PingData;

/**
 * Expose implemented methods.
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class ExposedCP extends CASSANDRA_PING implements ExposedPing
{
   @Override
   public void init() throws Exception
   {
      createRootDir();
   }

   @Override
   public void writeToFile(PingData data, String clustername)
   {
      super.writeToFile(data, clustername);
   }

   @Override
   public List<PingData> readAll(String clustername)
   {
      return super.readAll(clustername);
   }

   @Override
   public void remove(String clustername, Address addr)
   {
      super.remove(clustername, addr);
   }
}
