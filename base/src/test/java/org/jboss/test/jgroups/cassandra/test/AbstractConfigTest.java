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

import java.lang.reflect.Method;
import java.net.URL;
import java.util.List;

import org.jgroups.conf.ConfiguratorFactory;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.conf.ProtocolStackConfigurator;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.junit.Assert;
import org.junit.Test;

/**
 * Ping config test case.
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public abstract class AbstractConfigTest extends AbstractCassandraTest
{
   @Test
   public void testXmlConfig() throws Exception
   {
      URL url = getResource("etc/test-config.xml");
      Assert.assertNotNull(url);

      ProtocolStackConfigurator configurator = ConfiguratorFactory.getStackConfigurator(url);
      List<ProtocolConfiguration> configurations = configurator.getProtocolStack();
      Assert.assertNotNull(configurations);
      Assert.assertEquals(1, configurations.size());
      ProtocolConfiguration pc = configurations.get(0);

      Method createLayer = Configurator.class.getDeclaredMethod("createLayer", ProtocolStack.class, ProtocolConfiguration.class);
      Assert.assertNotNull(createLayer);
      createLayer.setAccessible(true);
      ProtocolStack protocolStack = new ProtocolStack();
      Protocol protocol = (Protocol) createLayer.invoke(null, protocolStack, pc);
      Assert.assertTrue(pingClass().isInstance(protocol));
   }

   protected abstract Class<? extends Protocol> pingClass();
}
