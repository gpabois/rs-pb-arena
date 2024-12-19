use std::{alloc::{alloc, dealloc, Layout}, cell::UnsafeCell,  marker::PhantomData, mem::MaybeUninit, ops::{Deref, DerefMut}, ptr::NonNull, sync::{atomic::{AtomicBool, AtomicI32, AtomicIsize, AtomicUsize, Ordering}, Arc}};

use pb_atomic_hash_map::AtomicHashMap;
use pb_atomic_linked_list::{AtomicLinkedList, AtomicQueue};

use crate::{ArenaBucketId, ArenaEntryId, ArenaId, ArenaPeriod};

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
            self.ptr.as_ref().wr_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);

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
            self.ptr.as_ref().wr_counter.swap(0, std::sync::atomic::Ordering::Relaxed);

        }
    }
}

struct Entry<T> {
    /// Dropping flag
    dropping: AtomicBool,
    /// Free flag
    free: AtomicBool,
    /// Write/Read counter
    wr_counter: AtomicI32,
    /// The current period of the entry
    period: AtomicUsize,
    /// The data stored
    data: T
}

impl<T> Entry<T> {
    unsafe fn new() -> Self {
        Self {
            period: AtomicUsize::new(0),
            dropping: AtomicBool::new(false),
            free: AtomicBool::new(true),
            wr_counter: AtomicI32::new(0),
            data: MaybeUninit::uninit().assume_init()
        }
    }

    /// Free the entry only if the period match.
    fn free_if_same_period(&self, period: ArenaPeriod) -> bool {
        if self.period.load(Ordering::Relaxed) == period {
            return self.free()
        }
        return false;
    }

    /// Free the entry 
    fn free(&self) -> bool {
        // already free
        if self.is_free() {
            return false;
        }

        // already borrowed
        if self.is_borrowed() {
            return false;
        }

        // cannot acquire the dropping possibility
        // mainly because another thread is already freeing this entry.
        if let Err(_) = self.dropping.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed) {
            return false;
        }

        // recheck, to avoid ABA issues.
        if self.is_borrowed() {
            self.dropping.store(false, Ordering::Release);
            return false;
        }

        // drop the data
        unsafe { (std::ptr::from_ref(&self.data) as *mut T).drop_in_place(); }

        // raise the free flag.
        self.free.store(true, Ordering::Relaxed);

        // release the "lock" dropping.
        self.dropping.store(false, Ordering::Release);
        
        return true;

    }

    /// Write data in the entry
    /// 
    /// Returns the period
    fn write(&mut self, data: T) -> Result<ArenaPeriod, T> {
        if let Ok(_) = self.free.compare_exchange(true, false, Ordering::SeqCst, Ordering::Relaxed) {
            self.data = data;
            self.free.store(false, Ordering::Release);
            return Ok(self.period.fetch_add(1, Ordering::Relaxed) + 1)
        }

        Err(data)
    }
    
    fn is_dropping(&self) -> bool {
        return self.dropping.load(Ordering::Relaxed)
    }

    fn is_free(&self) -> bool {
        return self.free.load(Ordering::Relaxed)
    }

    fn is_borrowed(&self) -> bool {
        return self.wr_counter.load(Ordering::Relaxed) != 0
    }

    fn borrow_mut_if_same_period<'a>(ptr: NonNull<Self>, period: &ArenaPeriod) -> Option<ArenaMutRef<'a, T>> {
        unsafe {
            let entry = ptr.as_ref();
            if entry.period.load(Ordering::Relaxed) != *period {
                return None
            }
            Self::borrow_mut(ptr)
        }
    }

    fn borrow_mut<'a>(ptr: NonNull<Self>) -> Option<ArenaMutRef<'a, T>> {
        unsafe {
            let entry = ptr.as_ref();

            if entry.is_dropping() {
                return None
            }

            if entry.is_free() {
                return None
            }

            if entry.wr_counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) == 0 {
                return Some(ArenaMutRef {
                    ptr,
                    _phantom: PhantomData
                })
            } else {
                entry.wr_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }

            None     
        }
    }

    fn borrow_if_same_period<'a>(ptr: NonNull<Self>, period: &ArenaPeriod) -> Option<ArenaRef<'a, T>> {
        unsafe {
            let entry = ptr.as_ref();
            
            if entry.period.load(Ordering::Relaxed) != *period {
                return None
            }
    
            Self::borrow(ptr)
        }
    }

    fn borrow<'a>(ptr: NonNull<Self>) -> Option<ArenaRef<'a, T>> {
        unsafe {
            let entry = ptr.as_ref();
            
            if entry.is_dropping() {
                return None
            }

            if entry.is_free() {
                return None
            }

            if entry.wr_counter.fetch_add(1, std::sync::atomic::Ordering::Acquire) >= 0 {
                return Some(ArenaRef {
                    ptr,
                    _phantom: PhantomData
                })
            } else {
                entry.wr_counter.fetch_sub(1, std::sync::atomic::Ordering::Release);
            }

            None
        }

    }
}

struct Bucket<T> {
    base: NonNull<[Entry<T>]>,
    tail: AtomicIsize,
    length: AtomicUsize,
    layout: Layout
}

impl<T: Sized> Drop for Bucket<T> {
    fn drop(&mut self) {
        unsafe {
            for cell in self.iter_entries_ptr() {
                cell.drop_in_place();
            }

            dealloc(self.base.cast().as_ptr(), self.layout);
        }
    }
}

impl<T> Bucket<T> {
    /// Creates a new bucket of a given size.
    fn new(size: usize) -> Self {
        unsafe {
            let layout = Layout::array::<Entry<T>>(size).unwrap();
            let raw_entries = alloc(layout).cast::<MaybeUninit<Entry<T>>>();
            let raw_entries_slice = std::ptr::slice_from_raw_parts_mut(raw_entries, size);
            let mut maybe_uninit_entries_ptr = NonNull::new(raw_entries_slice).unwrap();

            for entry in maybe_uninit_entries_ptr.as_mut() {
                entry.write(Entry::new());
            }

            let base = std::mem::transmute(maybe_uninit_entries_ptr);
            let last = AtomicIsize::new(-1);
            Self {base, tail: last, layout, length: AtomicUsize::new(0)}
        }
    }

    /// Iterate over all used (not free) entries.
    unsafe fn iter_entries_ptr(&self) -> impl Iterator<Item=NonNull<Entry<T>>> + '_ {
        (0..self.len())
            .flat_map(|entry| unsafe {
                self.get_entry_ptr_unchecked(&entry)
            })
            .filter(|entry| unsafe{ !entry.as_ref().is_free() })
    }

    /// Get the pointer to a specific *allocated* entry in the bucket.
    unsafe fn get_entry_ptr(&self, entry: &ArenaEntryId) -> Option<NonNull<Entry<T>>> {
        self
        .get_entry_ptr_unchecked(entry)
        .filter(|entry| !entry.as_ref().is_free())
    }

    /// Get any entry wether it's free or allocated.
    unsafe fn get_entry_ptr_unchecked(&self, entry: &ArenaEntryId) -> Option<NonNull<Entry<T>>> {
        unsafe {
            self.base
            .as_ptr()
            .as_mut()
            .unwrap()
            .get_mut(*entry)
            .map(std::ptr::from_mut)
            .map(NonNull::new)
            .flatten()
        }
    }

    /// Returns the number of allocated entries 
    fn len(&self) -> usize {
        return self.length.load(Ordering::Relaxed)
    }
    
    /// Returns the capacity of the bucket
    fn capacity(&self) -> usize {
        self.base.len()
    }

    fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }


    /// Allocate space to store the data
    /// 
    /// If no more room, returns the data, else returns the period and the entry.
    fn alloc(&self, data: T) -> Result<(ArenaPeriod, ArenaEntryId), T> {
        unsafe {
            if let Some((entry_id, mut entry)) = self.raw_alloc() {
                entry
                    .as_mut()
                    .write(data)
                    .map(|period| (period, entry_id))
                    .inspect(|_| {
                        self.length.fetch_add(1, Ordering::Relaxed);
                    })
            } else {
                Err(data)
            }
        }
    }

    /// Raw allocate space for an arena entry.
    unsafe fn raw_alloc(&self) -> Option<(ArenaEntryId, NonNull<Entry<T>>)> {
        if self.is_full() {
            return None
        }

        let new_tail = (self
            .tail
            .fetch_add(1, Ordering::Relaxed) + 1_isize
        ) as usize;
        
        unsafe {
            if let Some(entry_ptr) = self.get_entry_ptr_unchecked(&new_tail) {
                return Some((new_tail, entry_ptr))
            }
        }    

        return None 
    }

}

struct Inner<T> {
    buckets: UnsafeCell<AtomicLinkedList<Bucket<T>>>,
    bucket_size: usize,
    free_buckets: UnsafeCell<AtomicQueue<ArenaBucketId>>,
    free_entries: UnsafeCell<AtomicQueue<ArenaId>>,
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
            free_buckets: UnsafeCell::new(AtomicQueue::new()),
            free_entries: UnsafeCell::new(AtomicQueue::new()),
            cache: UnsafeCell::new(AtomicHashMap::new(cache_size))
        };

        Self(Arc::new(inner))
    }

    pub fn alloc(&mut self, mut data: T) -> ArenaId {
        unsafe {
            // Check for any freed entries (w/ Self::free)
            let free_entries = self.0.free_entries.get().as_mut().unwrap();
            while let Some(id) = free_entries.dequeue() {
                if let Some(mut entry) = self.get_entry_ptr_unchecked(&id) {
                    match entry.as_mut().write(data) {
                        Ok(period) => return ArenaId::new_with_period(id.bucket, id.entry, period),
                        Err(dat) => {
                            data = dat
                        },
                    }
                }
            }
            
            // Check for any non-fulled entries
            let free_blocks = self.0.free_buckets.get().as_mut().unwrap();
            while let Some(bucket_id) = free_blocks.dequeue() {
                if let Some(bucket) = self.get_bucket(&bucket_id) {
                    match bucket.alloc(data) {
                        Ok((period, entry)) => {
                            if !bucket.is_full() {
                                free_blocks.enqueue(bucket_id);
                            }
                            return ArenaId::new_with_period(bucket_id, entry, period)
                        },
                        Err(dat) => {
                            data = dat
                        },
                    }
                }
            }

            // We need to create a new bucket
            let buckets = self.0.buckets.get().as_mut().unwrap();
            let cache = self.0.cache.get().as_mut().unwrap();

            let bucket = Bucket::<T>::new(self.0.bucket_size);
            let (period, entry_id) = bucket.alloc(data).ok().unwrap();
            let (bucket_id, bucket_ptr) =  buckets.insert_and_returns_ptr(bucket);
            
            // Cache the bucket address
            cache.insert(bucket_id, bucket_ptr);

            // Add it to the free block register
            free_blocks.enqueue(bucket_id);

            return ArenaId::new_with_period(bucket_id, entry_id, period)
        }

    }

    /// Frees an object
    /// 
    /// Returns true if the object has been freed
    /// Returns false else
    pub fn free(&mut self, id: &ArenaId) -> bool {
        unsafe {
            let has_been_freed = if let Some(entry) = self.get_entry_ptr(id).map(|ptr| ptr.as_ref()) {
                match id.period {
                    Some(period) => entry.free_if_same_period(period),
                    None => entry.free()
                }
            } else {
                false
            };

            let free_entries =  self.0.free_entries.get().as_mut().unwrap();
            if has_been_freed {
                if let Some(bucket) = self.get_bucket(&id.bucket) {
                    bucket.length.fetch_sub(1, Ordering::Relaxed);
                }
                free_entries.enqueue(*id);
            }

            has_been_freed
        }
    }

    /// Borrow the entry
    pub fn borrow<'a>(&'a self, id: &ArenaId) -> Option<ArenaRef<'a, T>> {
        unsafe {
            self
            .get_entry_ptr(id)
            .and_then(|ptr| {
                match id.period {
                    Some(period) => Entry::borrow_if_same_period(ptr, &period),
                    None => Entry::borrow::<'a>(ptr),
                }
            })
        }
    }

    pub fn borrow_mut<'a>(&'a self, id: &ArenaId) -> Option<ArenaMutRef<'a, T>> {
        unsafe {
            self
            .get_entry_ptr(id)
            .and_then(|ptr| {
                match id.period {
                    Some(period) => Entry::borrow_mut_if_same_period(ptr, &period),
                    None => Entry::borrow_mut::<'a>(ptr)
                }
            })      
        }
    }

    /// Returns the pointer to an *allocated* entry.
    unsafe fn get_entry_ptr(&self, id: &ArenaId) -> Option<NonNull<Entry<T>>> {
        self
        .get_bucket(&id.bucket)
        .and_then(|bucket| bucket.get_entry_ptr(&id.entry))
    }

    unsafe fn get_entry_ptr_unchecked(&self, id : &ArenaId)  -> Option<NonNull<Entry<T>>> {
        self
        .get_bucket(&id.bucket)
        .and_then(|bucket| bucket.get_entry_ptr_unchecked(&id.entry))
    }

    /// Returns a reference to a bucket.
    unsafe fn get_bucket(&self, bucket_id: &ArenaBucketId) -> Option<&Bucket<T>> {
        unsafe {
            let cache = self.0.cache.get().as_ref().unwrap();
            cache
            .borrow(bucket_id)
            .map(|bucket_ptr| bucket_ptr.as_ref())
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use super::Arena;

    #[test]
    fn test_simple_alloc() {
        let mut arena = Arena::<u32>::new(100, 100);

        let id = arena.alloc(100);
         let maybe_borrowed = arena.borrow(&id);
        assert!(maybe_borrowed.is_some());
        let borrowed = maybe_borrowed.unwrap();
        assert_eq!(*borrowed, 100);
    }

    #[test]
    /// Freed entry cannot be borrowed
    fn test_cannot_borrow_after_drop() {
        let mut arena = Arena::<u32>::new(100, 100);
        let id = arena.alloc(100);
        
        assert!(arena.borrow(&id).is_some());
        assert!(arena.free(&id));
        assert!(arena.borrow(&id).is_none());
    }

    #[test]
    /// Freed entry must be reused for further allocations.
    fn test_free_and_realloc() {
        let mut arena = Arena::<u32>::new(100, 100);
        let id = arena.alloc(100);

        arena.free(&id);
        let id2 = arena.alloc(200);   

        assert_eq!(id.bucket, id2.bucket);
        assert_eq!(id.entry, id2.entry);
        assert_eq!(id.period.unwrap() + 1, id2.period.unwrap());

        assert!(arena.borrow(&id).is_none());
        assert_eq!(*arena.borrow(&id2).unwrap(), 200);
    }

    #[test]
    fn test_can_alloc_in_multiple_threads() {
        let arena = Arena::<u32>::new(100, 100);
        
        let mut arena_1 = arena.clone();
        let mut arena_2 = arena.clone();

        let j1 = thread::spawn(move || {
            for i in 0..=100 {
                arena_1.alloc(i);
            }
        });

        let j2 = thread::spawn(move || {
            for i in 0..=200 {
                arena_2.alloc(i);
            }
        });

        j1.join().unwrap();
        j2.join().unwrap();


    }
}