use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashSet;

pub trait RingHasher {
    fn hash<T: Hash>(&self, t: &T) -> u64;
    fn hash_vnode<T: Hash>(&self, t: &T, i: usize) -> u64 {
        self.hash(&(t, i))
    }
}

pub struct DefaultRingHasher(());
impl DefaultRingHasher {
    pub fn new() -> Self {
        DefaultRingHasher(())
    }
}
impl RingHasher for DefaultRingHasher {
    fn hash<T: Hash>(&self, t: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        t.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone)]
pub struct Node<K, V> {
    pub key: K,
    pub value: V,
    pub quantity: usize,
}
impl<K, V> Node<K, V>
    where K: Hash + Ord + Eq
{
    pub fn new(key: K, value: V, quantity: usize) -> Self {
        Node {
            key: key,
            value: value,
            quantity: quantity,
        }
    }
}

#[derive(Debug)]
struct VirtualNode<'a, K: 'a, V: 'a> {
    hash: u64,
    node: &'a Node<K, V>,
}

pub struct Candidates<'a, K: 'a, V: 'a> {
    start: usize,
    nodes: usize,
    ring: &'a [VirtualNode<'a, K, V>],
    seens: HashSet<usize>,
}
impl<'a, K: 'a, V: 'a> Candidates<'a, K, V>
    where K: Hash + Eq
{
    fn new(start: usize, nodes: usize, ring: &'a [VirtualNode<'a, K, V>]) -> Self {
        Candidates {
            start: start,
            nodes: nodes,
            ring: ring,
            seens: HashSet::new(),
        }
    }
}
impl<'a, K: 'a, V: 'a> Iterator for Candidates<'a, K, V>
    where K: Hash + Eq
{
    type Item = &'a Node<K, V>;
    fn next(&mut self) -> Option<Self::Item> {
        while self.seens.len() < self.nodes {
            if let Some(vn) = self.ring.get(self.start) {
                use std::mem;
                let key_addr: usize = unsafe { mem::transmute(&vn.node.key) };
                self.start += 1;
                if self.seens.contains(&key_addr) {
                    continue;
                }
                self.seens.insert(key_addr);
                return Some(&vn.node);
            } else {
                self.start = 0;
            }
        }
        None
    }
}

#[derive(Debug)]
pub struct StaticHashRing<'a, K: 'a, V: 'a, H> {
    hasher: H,
    nodes: Vec<Node<K, V>>,
    ring: Vec<VirtualNode<'a, K, V>>,
}
impl<'a, K: 'a, V: 'a, H: 'a + RingHasher> StaticHashRing<'a, K, V, H>
    where K: Hash + Ord + Eq
{
    pub fn new<I>(hasher: H, nodes: I) -> Self
        where I: Iterator<Item = Node<K, V>>
    {
        // TODO: sort and dedup
        let nodes = nodes.collect();
        let mut this = StaticHashRing {
            hasher: hasher,
            nodes: nodes,
            ring: Vec::new(),
        };
        this.build_ring();
        this
    }

    // TODO
    pub fn iter(&self) -> std::slice::Iter<Node<K, V>> {
        self.nodes.iter()
    }
    pub fn len(&self) -> usize {
        self.nodes.len()
    }
    pub fn calc_candidates<T: Hash>(&self, item: T) -> Candidates<K, V> {
        let item_hash = self.hasher.hash(&item);
        let start =
            self.ring.binary_search_by_key(&(item_hash, 0), |vn| (vn.hash, 1)).err().unwrap();
        Candidates::new(start, self.nodes.len(), &self.ring)
    }

    fn build_ring(&mut self) {
        assert!(self.ring.is_empty());

        let ring_size = self.nodes.iter().map(|n| n.quantity).sum();

        let mut ring = Vec::with_capacity(ring_size);
        for node in self.nodes.iter() {
            for i in 0..node.quantity {
                let hash = self.hasher.hash_vnode(&node.key, i);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut nodes = Vec::new();
        nodes.push(Node::new("foo", (), 10));
        nodes.push(Node::new("bar", (), 10));
        nodes.push(Node::new("baz", (), 10));
        let ring = StaticHashRing::new(DefaultRingHasher::new(), nodes.into_iter());
        println!("{:?}", ring.calc_candidates("aaa").collect::<Vec<_>>());
        println!("{:?}", ring.calc_candidates("bbb").collect::<Vec<_>>());
        assert!(false);
    }
}
