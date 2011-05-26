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

package org.jboss.test.jgroups.cassandra.test;

import java.util.List;

import org.jboss.test.jgroups.cassandra.support.ExposedCP;
import org.jgroups.Address;
import org.jgroups.protocols.PingData;
import org.jgroups.util.UUID;
import org.junit.Assert;
import org.junit.Test;

/**
 * Simple smoke test case.
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class SmokeTestCase extends CassandraTest
{
   @Test
   public void testBasicOps() throws Exception
   {
      if (isCassandraRunning() == false)
         return;

      // mock data
      Address address = UUID.randomUUID();
      PingData data = new PingData(address, null, true);

      ExposedCP ping = new ExposedCP();
      // TODO -- map config
      ping.init();
      try
      {
         ping.writeToFile(data, "test");

         List<PingData> datas = ping.readAll("test");
         Assert.assertNotNull(datas);
         Assert.assertEquals(1, datas.size());
         PingData pd = datas.get(0);
         Assert.assertEquals(data, pd);

         ping.remove("test", address);

         datas = ping.readAll("test");
         Assert.assertNotNull(datas);
         Assert.assertTrue(datas.isEmpty());
      }
      finally
      {
         ping.destroy();
      }
   }
}
