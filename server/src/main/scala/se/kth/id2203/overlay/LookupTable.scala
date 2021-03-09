/*
 * The MIT License
 *
 * Copyright 2017 Lars Kroll <lkroll@kth.se>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package se.kth.id2203.overlay;

import com.larskroll.common.collections._;
import java.util.Collection;
import se.kth.id2203.bootstrapping.NodeAssignment;
import se.kth.id2203.networking.NetAddress;

@SerialVersionUID(6322485231428233902L)
class LookupTable extends NodeAssignment with Serializable {

  val partitions = TreeSetMultiMap.empty[Int, NetAddress];

  def lookup(key: String): Iterable[NetAddress] = {
    val keyHash = key.hashCode();
    val partition = partitions.floor(keyHash) match {
      case Some(k) => k
      case None    => partitions.lastKey
    }
    return partitions(partition);
  }

  def getNodes(): Set[NetAddress] = partitions.foldLeft(Set.empty[NetAddress]) {
    case (acc, kv) => acc ++ kv._2
  }

  override def toString(): String = {
    val sb = new StringBuilder();
    sb.append("LookupTable(\n");
    sb.append(partitions.mkString(","));
    sb.append(")");
    return sb.toString();
  }

}

object LookupTable {
  /*
   * Generating the lookup table. Will be using a really simple partitioning system, that might not be
   * extremely dynamic but should give decent distribution among the different partitions.
   * Amount of partitions is set in the reference.conf file, as well as the replication degree.
   * Each partition is assigned a number from 0 to numberOfPartitions-1
   * A new key is assigned to a partition(and the partition is therefore responsible for such keys)
   * by which the key(Integer) %(modulo) numberOfPartitions == partitionNumber.
   * The replicationDegree sets the amount of servers assigned to the same partitions and there has to be
   * replicationDegree*numberOfPartitions amount of servers to generate the LookupTable.
   */
  def generate(nodes: Set[NetAddress], replicationDegree: Int): LookupTable = {
    val lut = new LookupTable();
    var i = 0;
    var partition = 0;

    for(node <- nodes){
      lut.partitions.put(partition, node);
      i += 1
      if(i >= replicationDegree){
        i = 0;
        partition += 1;
      }
    }
    lut;
  }
}
