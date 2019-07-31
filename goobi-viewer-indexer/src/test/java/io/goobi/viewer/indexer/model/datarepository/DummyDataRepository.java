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
package io.goobi.viewer.indexer.model.datarepository;

/**
 * Dummy class with usable space injection for testing.
 */
public class DummyDataRepository extends DataRepository {

    private long usableSpace;

    /**
     * @param path
     * @param usableSpace
     * @param buffer
     */
    public DummyDataRepository(String path, long usableSpace, long buffer) {
        super(path);
        super.setBuffer(buffer);
        this.usableSpace = usableSpace;
    }

    /**
     * @return the usableSpace
     */
    @Override
    public long getUsableSpace() {
        return usableSpace;
    }

    /**
     * @param usableSpace the usableSpace to set
     */
    public void setUsableSpace(long usableSpace) {
        this.usableSpace = usableSpace;
    }
}