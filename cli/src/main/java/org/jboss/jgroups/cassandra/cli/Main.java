/**
 * 
 */
package org.jboss.jgroups.cassandra.cli;

import java.util.Iterator;

import org.jboss.jgroups.cassandra.plugins.BaseCassandraSPI;
import org.jboss.jgroups.cassandra.spi.CassandraSPI;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;

/**
 * @author <a href="mailto:matejonnet@gmail.com">Matej Lazar</a>
 */
public class Main
{

	public static void main(String[] args) throws Exception {
		new Main(args);
	}

	Main(String[] args) throws Exception {
		JSAPResult commandLineOptions = parseParameters(args);
		
		BaseCassandraSPI cassandra = new BaseCassandraSPI();
		cassandra.setHost(commandLineOptions.getString("host"));
		cassandra.setPort(commandLineOptions.getInt("port"));

		String keyspaceName = commandLineOptions.getString("keyspaceName");
		String columnFamily = commandLineOptions.getString("columnFamily");
		
		String commandStr = commandLineOptions.getString("command");
		Command comamnd = Command.valueOf(commandStr.toUpperCase());
		
		switch(comamnd) {
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
	
	
	private JSAPResult parseParameters(String[] args) throws Exception {
		SimpleJSAP jsap = buildCommandLineOptions();

		JSAPResult config = jsap.parse(args);
		if (!config.success() || jsap.messagePrinted()) {
			Iterator<?> messageIterator = config.getErrorMessageIterator();
			while (messageIterator.hasNext()) System.err.println(messageIterator.next());
			System.err.println();
			System.err.println(jsap.getHelp());
			System.exit(0);
			//return null;
		}

		return config;
	}
	
	private SimpleJSAP buildCommandLineOptions() throws JSAPException {
		return new SimpleJSAP(
			"Utility to manage cassandra structure.",
			"",
			new Parameter[]{
				new FlaggedOption("host", JSAP.STRING_PARSER, "localhost",
						JSAP.NOT_REQUIRED, 'h', "host", "Cassandra host"),
				new FlaggedOption("port", JSAP.INTEGER_PARSER, "9160", 
						JSAP.NOT_REQUIRED, 'P', "port", "Cassandra port"),
				new FlaggedOption("keyspaceName", JSAP.STRING_PARSER, null,
						JSAP.REQUIRED, 'k', "keyspaceName", "Keyspace name"),
				new FlaggedOption("columnFamily", JSAP.STRING_PARSER, null,
						JSAP.NOT_REQUIRED, 'f', "columnFamily", "Column family"),
				new FlaggedOption("command", JSAP.STRING_PARSER, null,
						JSAP.REQUIRED, 'c', "command", "Use one of the commands: cf_create, cf_delete, ks_create, ks_delete")
				}
		);
	}
	
	private enum Command {
		KS_CREATE, KS_DELETE,
		CF_CREATE, CF_DELETE;
	}
}
