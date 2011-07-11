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
package org.jboss.jgroups.cassandra.cli;

import java.util.Iterator;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import org.jboss.jgroups.cassandra.plugins.BaseCassandraSPI;

/**
 * Cassandra SPI main, v7.
 *
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class Main
{
   public static void main(String[] args) throws Exception
   {
      new Main().execute(args);
   }

   protected void execute(String[] args) throws Exception
   {
      JSAPResult commandLineOptions = parseParameters(args);

      BaseCassandraSPI cassandra = new BaseCassandraSPI();
      cassandra.setHost(commandLineOptions.getString("host"));
      cassandra.setPort(commandLineOptions.getInt("port"));

      String keyspaceName = commandLineOptions.getString("keyspaceName");
      String columnFamily = commandLineOptions.getString("columnFamily");

      String commandStr = commandLineOptions.getString("command");
      Command comamnd = Command.valueOf(commandStr.toUpperCase());

      switch (comamnd)
      {
         case KS_CREATE:
            cassandra.createKeyspace(keyspaceName);
            break;

         case KS_DELETE:
            cassandra.dropKeyspace(keyspaceName);
            break;

         case CF_CREATE:
            cassandra.createColumnFamily(keyspaceName, columnFamily);
            break;

         case CF_DELETE:
            cassandra.dropColumnFamily(keyspaceName, columnFamily);
            break;
      }
   }

   private JSAPResult parseParameters(String[] args) throws Exception
   {
      SimpleJSAP jsap = buildCommandLineOptions();

      JSAPResult config = jsap.parse(args);
      if (!config.success() || jsap.messagePrinted())
      {
         Iterator<?> messageIterator = config.getErrorMessageIterator();
         while (messageIterator.hasNext())
            System.err.println(messageIterator.next());
         System.err.println();
         System.err.println(jsap.getHelp());
         System.exit(0);
         //return null;
      }

      return config;
   }

   private SimpleJSAP buildCommandLineOptions() throws JSAPException
   {
      return new SimpleJSAP(
            "Utility to manage Cassandra structure.",
            "",
            new Parameter[]{
                  new FlaggedOption("host", JSAP.STRING_PARSER, "localhost", JSAP.NOT_REQUIRED, 'h', "host", "Cassandra host"),
                  new FlaggedOption("port", JSAP.INTEGER_PARSER, "9160", JSAP.NOT_REQUIRED, 'P', "port", "Cassandra port"),
                  new FlaggedOption("keyspaceName", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'k', "keyspaceName", "Keyspace name"),
                  new FlaggedOption("columnFamily", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'f', "columnFamily", "Column family"),
                  new FlaggedOption("command", JSAP.STRING_PARSER, null, JSAP.REQUIRED, 'c', "command", "Use one of the commands: cf_create, cf_delete, ks_create, ks_delete")
            }
      );
   }

   private enum Command
   {
      KS_CREATE,
      KS_DELETE,
      CF_CREATE,
      CF_DELETE
   }
}
