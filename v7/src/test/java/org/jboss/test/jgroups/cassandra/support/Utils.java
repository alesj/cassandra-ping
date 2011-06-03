/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.logging.Logger;

import org.apache.thrift.transport.TSocket;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class Utils
{
   private static Logger log = Logger.getLogger(Utils.class.getName());

   public static boolean isCassandraRunning(String host, int port)
   {
      try
      {
         TSocket ts = new TSocket(host, port);
         ts.open();
         try
         {
            return true;
         }
         finally
         {
            ts.close();
         }
      }
      catch (Throwable t)
      {
         log.warning(t.getMessage());
         return false;
      }
   }
}
