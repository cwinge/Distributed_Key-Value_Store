package se.kth.id2203.overlay;

import se.kth.id2203.networking.NetAddress;

class ReplicationGroup(nodesIn: Set[NetAddress]){
  var nodes = nodesIn;

  def getNodes(): Set[NetAddress] ={
    nodes;
  }

  def removeNode(node: NetAddress): Unit ={
    nodes -= node;
  }

  def addNode(node: NetAddress): Unit ={
    nodes += node;
  }
}