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

import org.jgroups.annotations.Experimental;
import org.jgroups.protocols.Discovery;
import org.jgroups.util.Promise;

/**
 * Simple discovery protocol which uses a Apache Cassandra DB. The local
 * address information, e.g. UUID and physical addresses mappings are written to the DB and the content is read and
 * added to our transport's UUID-PhysicalAddress cache.<p/>
 * The design is at doc/design/CASSANDRA_PING.txt
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
@Experimental
public class CASSANDRA_PING extends Discovery {
    @Override
    public void sendGetMembersRequest(String cluster_name, Promise promise, boolean return_views_only) throws Exception {
        // TODO
    }
}
