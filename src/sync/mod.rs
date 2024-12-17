use std::{alloc::{alloc, dealloc, Layout}, cell::UnsafeCell, marker::PhantomData, mem::MaybeUninit, ops::{Deref, DerefMut}, ptr::NonNull, sync::{atomic::{AtomicI32, AtomicIsize}, Arc}};

use pb_atomic_hash_map::AtomicHashMap;
use pb_atomic_linked_list::{prelude::AtomicLinkedList as _, AtomicLinkedList, AtomicQueue};

use crate::{ArenaBucketId, ArenaCellId, ArenaId};

pub struct ArenaRef<'a, T> {
    _phantom: PhantomData<&'a ()>,
    ptr: NonNull<Entry<T>>
}

impl<'a, T> Deref for ArenaRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &self.ptr.as_ref().data
        }

    }
}

impl<'a, T> Drop for ArenaRef<'a, T> {
    fn drop(&mut self) {
        unsafe {
            self.ptr.as_ref().wr_lock.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

        }
    }
}

pub struct ArenaMutRef<'a, T> {
    _phantom: PhantomData<&'a ()>,
    ptr: NonNull<Entry<T>>
}

impl<'a, T> Deref for ArenaMutRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            &self.ptr.as_ref().data
        }
    }
}


impl<'a, T> DerefMut for ArenaMutRef<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {&mut self.ptr.as_mut().data}
    }
}


impl<'a, T> Drop for ArenaMutRef<'a, T> {
    fn drop(&mut self) {
        unsafe {
            self.ptr.as_ref().wr_lock.swap(0, std::sync::atomic::Ordering::Relaxed);

        }
    }
}

struct Entry<T> {
    wr_lock: AtomicI32,
    data: T
}

impl<T> Entry<T> {
    unsafe fn new() -> Self {
        Self {
            wr_lock: AtomicI32::new(0),
            data: MaybeUninit::uninit().assume_init()
        }
    }

    fn write(&mut self, data: T) {
        self.data = data;
    }

    fn borrow_mut<'a>(ptr: NonNull<Self>) -> Option<ArenaMutRef<'a, T>> {
        unsafe {
            if ptr.as_ref().wr_lock.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                return Some(ArenaMutRef {
                    ptr,
                    _phantom: PhantomData
                })
            } else {
                ptr.as_ref().wr_lock.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            None     
        }
    }
    fn borrow<'a>(ptr: NonNull<Self>) -> Option<ArenaRef<'a, T>> {
        unsafe {
            if ptr.as_ref().wr_lock.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 0 {
                return Some(ArenaRef {
                    ptr,
                    _phantom: PhantomData
                })
            } else {
                ptr.as_ref().wr_lock.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }

            None
        }

    }
}

struct Bucket<T> {
    base: NonNull<[Entry<T>]>,
    last: AtomicIsize,
    layout: Layout
}

impl<T: Sized> Drop for Bucket<T> {
    fn drop(&mut self) {
        unsafe {
            for cell in self.iter_cells() {
                cell.drop_in_place();
            }

            dealloc(self.base.cast().as_ptr(), self.layout);
        }
    }
}

impl<T> Bucket<T> {
    fn new(size: usize) -> Self {
        unsafe {
            let layout = Layout::array::<Entry<T>>(size).unwrap();
            let raw_entries = alloc(layout).cast::<MaybeUninit<Entry<T>>>();
            let raw_entries_slice = std::ptr::slice_from_raw_parts_mut(raw_entries, size);
            let mut entries_ptr = NonNull::new(raw_entries_slice).unwrap();

            for entry in entries_ptr.as_mut() {
                entry.write(Entry::new());
            }

            let base = std::mem::transmute(entries_ptr);
            let last = AtomicIsize::new(-1);
            Self {base, last, layout}
        }
    }

    fn iter_cells(&self) -> impl Iterator<Item=NonNull<Entry<T>>> + '_ {
        (0..self.len())
            .map(ArenaCellId)
            .flat_map(|cell_id: ArenaCellId| unsafe{
                self.get_cell_unchecked(&cell_id)
            })
    }

    unsafe fn get_cell_ptr(&self, cell_id: &ArenaCellId) -> Option<NonNull<Entry<T>>> {
        if cell_id.0 >= self.len() {
            return None
        }

        self.get_cell_unchecked(cell_id)
    }

    unsafe fn get_cell_unchecked(&self, cell_id: &ArenaCellId) -> Option<NonNull<Entry<T>>> {
        unsafe {
            self.base
            .as_ptr()
            .as_mut()
            .unwrap()
            .get_mut(cell_id.0)
            .map(std::ptr::from_mut)
            .map(NonNull::new)
            .flatten()
        }
    }
    fn len(&self) -> usize {
        let offset = self.last.load(std::sync::atomic::Ordering::Relaxed);
        if offset < 0 { return 0 } 
        if offset >= self.capacity() as isize { return self.capacity() }
        return offset as usize; 
    }
    
    
    fn capacity(&self) -> usize {
        self.base.len()
    }

    fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    fn alloc(&self, data: T) -> Option<ArenaCellId> {
        unsafe {
            self
            .raw_alloc()
            .map(|(cell_id, mut cell)| {
                cell.as_mut().write(data);
                cell_id
            })
        }
    }

    /// Raw allocate space for an arena cell.
    unsafe fn raw_alloc(&self) -> Option<(ArenaCellId, NonNull<Entry<T>>)> {
        if self.is_full() {
            return None
        }

        let offset = (self
            .last.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1isize) as usize;
        
        unsafe {
            if let Some(cell_ptr) = self.get_cell_unchecked(&ArenaCellId(offset)) {
                return Some((ArenaCellId(offset), cell_ptr))
            }
        }    

        return None 
    }

}

struct Inner<T> {
    buckets: UnsafeCell<AtomicLinkedList<Bucket<T>>>,
    bucket_size: usize,
    free_blocks: UnsafeCell<AtomicQueue<ArenaBucketId>>,
    cache: UnsafeCell<AtomicHashMap<ArenaBucketId, NonNull<Bucket<T>>>>
}

unsafe impl<T> Sync for Inner<T> {}
unsafe impl<T> Send for Inner<T> {}

/// A thread-safe arena allocator.
pub struct Arena<T>(Arc<Inner<T>>);

impl<T> Clone for Arena<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Arena<T> {
    pub fn new(bucket_size: usize, cache_size: usize) -> Self {
        let inner = Inner {
            buckets: UnsafeCell::new(AtomicLinkedList::new()),
            bucket_size,
            free_blocks: UnsafeCell::new(AtomicQueue::new()),
            cache: UnsafeCell::new(AtomicHashMap::new(cache_size))
        };

        Self(Arc::new(inner))
    }

    pub fn alloc(&mut self, data: T) -> ArenaId {
        unsafe {
            let free_blocks = self.0.free_blocks.get().as_mut().unwrap();

            // We check the free blocks queue
            if let Some(block_id) = free_blocks.dequeue() {
                if let Some(block) = self.get_bucket_ptr(&block_id) {
                    if let Some((cell_id, mut cell)) = block.raw_alloc() {
                        if !block.is_full() {
                            free_blocks.enqueue(block_id);
                        }
                        
                        cell.as_mut().write(data);
                        return ArenaId {block_id, cell_id};
                    }
                }      
            }

            // We need to create a new block
            let buckets = self.0.buckets.get().as_mut().unwrap();
            let cache = self.0.cache.get().as_mut().unwrap();


            let bucket = Bucket::<T>::new(self.0.bucket_size);
            let cell_id = bucket.alloc(data).unwrap();
            let (block_id_raw, bucket_ptr) =  buckets.insert_and_returns_ptr(bucket);
            let block_id = ArenaBucketId(block_id_raw);
            // Cache the bucket address
            cache.insert(block_id, bucket_ptr);

            // Add it to the free block register
            free_blocks.enqueue(block_id);

            return ArenaId { block_id, cell_id }
        }

    }

    pub fn borrow<'a>(&'a self, id: &ArenaId) -> Option<ArenaRef<'a, T>> {
        unsafe {
            self.get_cell_ptr(id).and_then(|cell| Entry::borrow::<'a>(cell))
        }
    }

    pub fn borrow_mut<'a>(&'a self, id: &ArenaId) -> Option<ArenaMutRef<'a, T>> {
        unsafe {
            self.get_cell_ptr(id).and_then(|cell| Entry::borrow_mut::<'a>(cell))      
        }
    }

    unsafe fn get_cell_ptr(&self, id: &ArenaId) -> Option<NonNull<Entry<T>>> {
        self.get_bucket_ptr(&id.block_id).and_then(|bucket| bucket.get_cell_ptr(&id.cell_id))
    }

    unsafe fn get_bucket_ptr(&self, block_id: &ArenaBucketId) -> Option<&Bucket<T>> {
        unsafe {
            self.0.cache.get().as_ref().unwrap().borrow(block_id).map(|bucket_ptr| bucket_ptr.as_ref())
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use super::Arena;

    #[test]
    fn test_can_alloc_in_multiple_threads() {
        let arena = Arena::<u32>::new(100, 100);
        
        let mut arena_1 = arena.clone();
        let mut arena_2 = arena.clone();

        let j1 = thread::spawn(move || {
            for i in 0..=1000 {
                arena_1.alloc(i);
            }
        });

        let j2 = thread::spawn(move || {
            for i in 0..=2000 {
                arena_2.alloc(i);
            }
        });

        j1.join().unwrap();
        j2.join().unwrap();


    }
}