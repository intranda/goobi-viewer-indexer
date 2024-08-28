/**
 * This file is part of the Goobi Solr Indexer - a content indexing tool for the Goobi viewer and OAI-PMH/SRU interfaces.
 *
 * Visit these websites for more information.
 *          - http://www.intranda.com
 *          - http://digiverso.com
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free
 * Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package io.goobi.viewer.indexer.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.solr.common.SolrInputDocument;

public class PhysicalElement implements Serializable, Comparable<PhysicalElement> {

    private static final long serialVersionUID = -5701506008633643945L;

    private final int order;
    private final SolrInputDocument doc = new SolrInputDocument();
    private final List<PhysicalElement> shapes = new ArrayList<>();
    private final List<GroupedMetadata> groupedMetadata = new ArrayList<>();

    @Override
    public int hashCode() {
        return Objects.hash(groupedMetadata, order, shapes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PhysicalElement other = (PhysicalElement) obj;
        return Objects.equals(groupedMetadata, other.groupedMetadata) && order == other.order && Objects.equals(shapes, other.shapes);
    }

    @Override
    public int compareTo(PhysicalElement page) {
        return Integer.compare(order, page.order);
    }

    /**
     * 
     * @param order
     */
    public PhysicalElement(int order) {
        this.order = order;
    }

    /**
     * @return the doc
     */
    public SolrInputDocument getDoc() {
        return doc;
    }

    /**
     * @return the shapes
     */
    public List<PhysicalElement> getShapes() {
        return shapes;
    }

    /**
     * @return the groupedMetadata
     */
    public List<GroupedMetadata> getGroupedMetadata() {
        return groupedMetadata;
    }

    /**
     * @return the order
     */
    public int getOrder() {
        return order;
    }
}
