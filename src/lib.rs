//! An implementation of Consistent hashing algorithm.
//!
//! Currently this crate only provides `StaticHashRing` which
//! represents statically built, virtual node based hash rings.
//!
//! # Examples
//!
//! ```
//! use consistent_hash::{Node, StaticHashRing, DefaultHash};
//!
//! let nodes = vec![
//!     Node::new("foo").quantity(5),
//!     Node::new("bar").quantity(5),
//!     Node::new("baz").quantity(1),
//!     Node::new("baz").quantity(2), // ignored (duplicate key)
//! ];
//!
//! let ring = StaticHashRing::new(DefaultHash, nodes.into_iter());
//! assert_eq!(ring.len(), 11);        // virtual node count
//! assert_eq!(ring.nodes().len(), 3); // real node count
//!
//! assert_eq!(ring.calc_candidates(&"aa").map(|n| &n.key).collect::<Vec<_>>(),
//!            [&"bar", &"foo", &"baz"]);
//! assert_eq!(ring.calc_candidates(&"bb").map(|n| &n.key).collect::<Vec<_>>(),
//!            [&"foo", &"bar", &"baz"]);
//! ```
#![warn(missing_docs)]
extern crate siphasher;
extern crate splay_tree;

use std::hash::{Hash, Hasher};
use siphasher::sip::SipHasher13;
use splay_tree::SplaySet;

/// A node in a hash ring.
///
/// # Examples
///
/// ```
/// use consistent_hash::Node;
///
/// // Constructs directly.
/// let node0 = Node {
///     key: "foo",
///     value: 123,
///     quantity: 7,
/// };
///
/// // Conscructs via building functions.
/// let node1 = Node::new("foo").value(123).quantity(7);
///
/// assert_eq!(node0, node1);
/// ```
#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct Node<K, V> {
    /// The key of this node.
    pub key: K,

    /// The value of this node.
    pub value: V,

    /// The quantity of virtual nodes assigned for this node.
    pub quantity: usize,
}
impl<K> Node<K, ()> {
    /// Makes a new `Node` instance.
    ///
    /// The value of it is `()` and the quantity is set to `1`.
    pub fn new(key: K) -> Self {
        Node {
            key: key,
            value: (),
            quantity: 1,
        }
    }
}
impl<K, V> Node<K, V> {
    /// Makes a new `Node` instance which has the value `value`.
    ///
    /// Other fields of the returning node is the same as `self`.
    pub fn value<U>(self, value: U) -> Node<K, U> {
        Node {
            key: self.key,
            value: value,
            quantity: self.quantity,
        }
    }

    /// Sets the quantity of this node to `quantity`.
    pub fn quantity(mut self, quantity: usize) -> Node<K, V> {
        self.quantity = quantity;
        self
    }
}

#[derive(Debug)]
struct VirtualNode<'a, K: 'a, V: 'a> {
    hash: u64,
    node: &'a Node<K, V>,
}

/// This trait allows calculating hash codes for virtual nodes and items.
pub trait RingHash {
    /// Calculates the hash code of the item.
    fn hash_item<T: Hash>(&self, item: &T) -> u64;

    /// Calculates the hash code of the virtual node.
    ///
    /// The default implementation is `self.hash_item(&(node_key, vnode_seq))`.
    fn hash_vnode<K: Hash>(&self, node_key: &K, vnode_seq: usize) -> u64 {
        self.hash_item(&(node_key, vnode_seq))
    }
}

/// The default `RingHash` implementation.
///
/// The hashing function used by this implementation is `SipHash 1-3`.
#[derive(Debug, Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct DefaultHash;
impl RingHash for DefaultHash {
    fn hash_item<T: Hash>(&self, item: &T) -> u64 {
        let mut hasher = SipHasher13::new();
        item.hash(&mut hasher);
        hasher.finish()
    }
}

/// A hash ring which is built statically.
///
/// Once a ring instance is created, it cannot be modified afterwards.
///
/// # Examples
///
/// ```
/// use consistent_hash::{Node, StaticHashRing, DefaultHash};
///
/// let nodes = vec![
///     Node::new("foo").quantity(5),
///     Node::new("bar").quantity(5),
///     Node::new("baz").quantity(1),
///     Node::new("baz").quantity(2), // ignored (duplicate key)
/// ];
///
/// let ring = StaticHashRing::new(DefaultHash, nodes.into_iter());
/// assert_eq!(ring.len(), 11);        // virtual node count
/// assert_eq!(ring.nodes().len(), 3); // real node count
///
/// assert_eq!(ring.calc_candidates(&"aa").map(|n| &n.key).collect::<Vec<_>>(),
///            [&"bar", &"foo", &"baz"]);
/// assert_eq!(ring.calc_candidates(&"bb").map(|n| &n.key).collect::<Vec<_>>(),
///            [&"foo", &"bar", &"baz"]);
/// ```
#[derive(Debug)]
pub struct StaticHashRing<'a, K: 'a, V: 'a, H> {
    hash: H,
    nodes: Vec<Node<K, V>>,
    ring: Vec<VirtualNode<'a, K, V>>,
}
impl<'a, K: 'a, V: 'a, H> StaticHashRing<'a, K, V, H>
    where K: Hash + Eq + Ord,
          H: RingHash
{
    /// Makes a new `StaticHashRing` instance.
    ///
    /// If multiple nodes which have the same key are contained in `nodes`,
    /// all of those nodes but first one are ignored.
    pub fn new<I>(hash: H, nodes: I) -> Self
        where I: Iterator<Item = Node<K, V>>
    {
        let mut nodes = nodes.collect::<Vec<_>>();

        // Removes duplicate nodes
        nodes.sort_by(|a, b| a.key.cmp(&b.key));
        for i in (1..nodes.len()).rev() {
            if nodes[i].key == nodes[i - 1].key {
                nodes.swap_remove(i);
            }
        }

        let mut this = StaticHashRing {
            hash: hash,
            nodes: nodes,
            ring: Vec::new(),
        };
        this.build_ring();
        this
    }

    fn build_ring(&mut self) {
        assert!(self.ring.is_empty());

        let ring_size = self.nodes.iter().map(|n| n.quantity).sum();

        let mut ring = Vec::with_capacity(ring_size);
        for node in self.nodes.iter() {
            for i in 0..node.quantity {
                let hash = self.hash.hash_vnode(&node.key, i);
                let node = unsafe { &*(node as *const _) as &'a _ };
                let vnode = VirtualNode {
                    hash: hash,
                    node: node,
                };
                ring.push(vnode);
            }
        }
        self.ring = ring;
        self.ring.sort_by_key(|vn| (vn.hash, &vn.node.key));
    }
}
impl<'a, K: 'a, V: 'a, H> StaticHashRing<'a, K, V, H>
    where H: RingHash
{
    /// Returns the candidate nodes for `item`.
    ///
    /// The higher priority node is located in front of the returned candidate sequence.
    pub fn calc_candidates<T: Hash>(&self, item: &T) -> Candidates<K, V> {
        let item_hash = self.hash.hash_item(item);
        let start =
            self.ring.binary_search_by_key(&(item_hash, 0), |vn| (vn.hash, 1)).err().unwrap();
        Candidates::new(start, self.nodes.len(), &self.ring)
    }

    /// Removes the virtual node which associated to `item` and returns the reference to the node.
    pub fn take<T: Hash>(&mut self, item: &T) -> Option<&Node<K, V>> {
        self.take_if(item, |_| true)
    }

    /// Removes the virtual node which has the highest priority for `item`
    /// among satisfying the predicate `f`,
    /// and returns the reference to the node.
    pub fn take_if<T: Hash, F>(&mut self, item: &T, f: F) -> Option<&Node<K, V>>
        where F: Fn(&Node<K, V>) -> bool
    {
        let item_hash = self.hash.hash_item(item);
        let start =
            self.ring.binary_search_by_key(&(item_hash, 0), |vn| (vn.hash, 1)).err().unwrap();
        let vnode_index = CandidateVnodes::new(start, self.nodes.len(), &self.ring)
            .find(|&i| f(&self.ring[i].node));
        if let Some(index) = vnode_index {
            Some(self.ring.remove(index).node)
        } else {
            None
        }
    }
}
impl<'a, K: 'a, V: 'a, H> StaticHashRing<'a, K, V, H> {
    /// Returns the count of the virtual nodes in this ring.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns the reference to the real nodes contained in this ring.
    ///
    /// Note that the order of the returning nodes are undefined.
    pub fn nodes(&self) -> &[Node<K, V>] {
        &self.nodes[..]
    }
}

/// An iterator which represents a sequence of the candidate nodes for an item.
///
/// The higher priority node is placed in front of this sequence.
///
/// This is created by calling `StaticHashRing::calc_candidates` method.
pub struct Candidates<'a, K: 'a, V: 'a>(CandidateVnodes<'a, K, V>);
impl<'a, K: 'a, V: 'a> Candidates<'a, K, V> {
    fn new(start: usize, nodes: usize, ring: &'a [VirtualNode<'a, K, V>]) -> Self {
        Candidates(CandidateVnodes::new(start, nodes, ring))
    }
}
impl<'a, K: 'a, V: 'a> Iterator for Candidates<'a, K, V> {
    type Item = &'a Node<K, V>;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|i| self.0.ring[i].node)
    }
}

struct CandidateVnodes<'a, K: 'a, V: 'a> {
    start: usize,
    nodes: usize,
    ring: &'a [VirtualNode<'a, K, V>],
    seens: SplaySet<usize>,
}
impl<'a, K: 'a, V: 'a> CandidateVnodes<'a, K, V> {
    fn new(start: usize, nodes: usize, ring: &'a [VirtualNode<'a, K, V>]) -> Self {
        CandidateVnodes {
            start: start,
            nodes: nodes,
            ring: ring,
            seens: Default::default(),
        }
    }
}
impl<'a, K: 'a, V: 'a> Iterator for CandidateVnodes<'a, K, V> {
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {
        while self.seens.len() < self.nodes {
            let index = self.start;
            if let Some(vn) = self.ring.get(index) {
                let key_addr: usize = unsafe { std::mem::transmute(&vn.node.key) };
                self.start += 1;
                if self.seens.contains(&key_addr) {
                    continue;
                }
                self.seens.insert(key_addr);
                return Some(index);
            } else {
                self.start = 0;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut nodes = Vec::new();
        nodes.push(Node::new("foo").quantity(5));
        nodes.push(Node::new("bar").quantity(5));
        nodes.push(Node::new("baz").quantity(1));
        nodes.push(Node::new("baz").quantity(2)); // ignored (duplicate key)

        let ring = StaticHashRing::new(DefaultHash, nodes.into_iter());
        assert_eq!(ring.len(), 11);
        assert_eq!(ring.nodes().len(), 3);

        assert_eq!(ring.calc_candidates(&"aa").map(|n| &n.key).collect::<Vec<_>>(),
                   [&"bar", &"foo", &"baz"]);
        assert_eq!(ring.calc_candidates(&"bb").map(|n| &n.key).collect::<Vec<_>>(),
                   [&"foo", &"bar", &"baz"]);
    }

    #[test]
    fn take_works() {
        let mut nodes = Vec::new();
        nodes.push(Node::new("foo").quantity(5));
        nodes.push(Node::new("bar").quantity(5));
        nodes.push(Node::new("baz").quantity(1));

        let mut ring = StaticHashRing::new(DefaultHash, nodes.into_iter());
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "bar");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "foo");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "bar");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "bar");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "foo");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "foo");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "foo");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "bar");
        assert_eq!(ring.take(&"aa").map(|n| n.key).unwrap(), "baz");
    }
}
