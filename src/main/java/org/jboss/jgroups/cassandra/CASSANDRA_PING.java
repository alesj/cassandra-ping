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

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.PhysicalAddress;
import org.jgroups.annotations.Experimental;
import org.jgroups.protocols.Discovery;
import org.jgroups.protocols.FILE_PING;
import org.jgroups.protocols.PingData;
import org.jgroups.protocols.PingHeader;
import org.jgroups.util.Promise;
import org.jgroups.util.UUID;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Simple discovery protocol which uses a Apache Cassandra DB. The local
 * address information, e.g. UUID and physical addresses mappings are written to the DB and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/CASSANDRA_PING.txt<p/>
 *
 * todo: READ below
 * A possible mapping to Cassandra could be to take the clustername-Address (where Address is a UUID), e.g.
 * "MyCluster-15524-355142-335-1dd3" and associate the logical name and physical address with this key as value.<p/>
 * If Cassandra provides search, then to find all nodes in cluster "MyCluster", we'd have to grab all keys starting
 * with "MyCluster". If search is not provided, then this is not good as it requires a linear iteration of all keys...
 * <p/>
 * As an alternative, maybe a Cassandra table can be created named the same as the cluster (e.g. "MyCluster"). Then the
 * keys would be the addresses (UUIDs)
 *
 *
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 * @author Bela Ban
 */
@Experimental
public class CASSANDRA_PING extends FILE_PING {
    @Override
    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception {
        List<PingData> existing_mbrs=readAll(cluster_name);
        PhysicalAddress physical_addr=(PhysicalAddress)down(new Event(Event.GET_PHYSICAL_ADDRESS, local_addr));
        List<PhysicalAddress> physical_addrs= Arrays.asList(physical_addr);
        PingData data=new PingData(local_addr, null, false, UUID.get(local_addr), physical_addrs);

        // If we don't find any files, return immediately
        if(existing_mbrs.isEmpty()) {
            if(promise != null) {
                promise.setResult(null);
            }
        }
        else {

            // 1. Send GET_MBRS_REQ message to members listed in the file
            for(PingData tmp: existing_mbrs) {
                Collection<PhysicalAddress> dests=tmp != null? tmp.getPhysicalAddrs() : null;
                if(dests == null)
                    continue;
                for(final PhysicalAddress dest: dests) {
                    if(dest == null || dest.equals(physical_addr))
                        continue;
                    PingHeader hdr=new PingHeader(PingHeader.GET_MBRS_REQ, data, cluster_name);
                    hdr.return_view_only=return_views_only;
                    final Message msg=new Message(dest);
                    msg.setFlag(Message.OOB);
                    msg.putHeader(this.id, hdr); // needs to be getName(), so we might get "MPING" !
                    // down_prot.down(new Event(Event.MSG,  msg));
                    if(log.isTraceEnabled())
                        log.trace("[FIND_INITIAL_MBRS] sending PING request to " + msg.getDest());
                    timer.execute(new Runnable() {
                        public void run() {
                            try {
                                down_prot.down(new Event(Event.MSG, msg));
                            }
                            catch(Exception ex){
                                if(log.isErrorEnabled())
                                    log.error("failed sending discovery request to " + dest, ex);
                            }
                        }
                    });
                }
            }
        }

        // Write my own data to file
        writeToFile(data, cluster_name);
    }


    @Override
    protected List<PingData> readAll(String clustername) {

        // todo: return information on all nodes associated with 'clustername'
        // Probably a Cassandra GET

        return null;
    }

    @Override
    protected void remove(String clustername, Address addr) {
        // todo: remove the entry for 'addr' in 'clustername'
        // Probably a Cassandra REMOVE
    }

    @Override
    protected void writeToFile(PingData data, String clustername) {
        // todo: add 'data' to the entry with 'clustername'
        // Probably a Cassandra PUT
    }

    @Override
    protected void createRootDir() {
        // todo: create an entry in Cassandra based on 'location' (maybe this is not needed)
    }
}
