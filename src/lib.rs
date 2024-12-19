//! ID based arena for graph operations.

use std::{alloc::{alloc, dealloc, Layout}, cell::UnsafeCell, iter::Enumerate, marker::PhantomData, mem::MaybeUninit, ops::{Deref, DerefMut}, ptr::NonNull};

pub mod sync;

type ArenaPeriod = usize;
type ArenaEntryId = usize;
type ArenaBucketId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub struct ArenaId {
    pub(crate) period: Option<usize>,
    pub(crate) bucket: usize,
    pub(crate) entry: usize
}
impl Ord for ArenaId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.bucket != other.bucket {
            return self.bucket.cmp(&other.bucket);
        }

        return self.entry.cmp(&other.entry);
    }
}
impl ArenaId {
    fn new_with_period(bucket: ArenaBucketId, entry: ArenaEntryId, period: ArenaPeriod) -> Self {
        Self {bucket, entry, period: Some(period)}
    }
}

pub struct ArenaMutRef<'a, T: Sized> {
    _phantom: PhantomData<&'a ()>,
    ptr: NonNull<Entry<T>>
}
impl<T: Sized> Deref for ArenaMutRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.ptr.as_ref().data.get().as_ref().unwrap().assume_init_ref()
        }
    }
}
impl<T: Sized> DerefMut for ArenaMutRef<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.ptr.as_mut().data.get_mut().assume_init_mut()
        }
    }
}
impl<T: Sized> Drop for ArenaMutRef<'_, T> {
    fn drop(&mut self) {
        unsafe {
            *self.ptr.as_mut().wr_counter.get_mut() += 1;
        }
    }
}

pub struct ArenaRef<'a, T: Sized> {
    _phantom: PhantomData<&'a ()>,
    ptr: NonNull<Entry<T>>
}
impl<T: Sized> Deref for ArenaRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.ptr.as_ref().data.get().as_ref().unwrap().assume_init_ref()
        }
    }
}
impl<T: Sized> Drop for ArenaRef<'_, T> {
    fn drop(&mut self) {
        unsafe {
            *self.ptr.as_mut().wr_counter.get_mut() -= 1;
        }
    }
}

struct Entry<T: Sized> {
    wr_counter: UnsafeCell<i32>,
    period: UnsafeCell<usize>,
    free: UnsafeCell<bool>,
    data: UnsafeCell<MaybeUninit<T>>
}

impl<T: Sized> Drop for Entry<T> {
    fn drop(&mut self) {
        if !self.is_free() {
            unsafe {
                self.data.get_mut().assume_init_drop();
            }
        }
    }
}

impl<T: Sized> Entry<T> {
    pub unsafe fn new() -> Self {
        Self {
            wr_counter: UnsafeCell::new(0),
            period: UnsafeCell::new(0),
            free: UnsafeCell::new(true),
            data: UnsafeCell::new(MaybeUninit::<T>::uninit())
        }
    }

    fn borrow_mut<'a>(ptr: NonNull<Self>) -> Option<ArenaMutRef<'a, T>> {
        unsafe {
            let entry = ptr.as_ptr().as_mut().unwrap();
            let wr_counter = entry.wr_counter.get_mut();
            (*wr_counter == 0).then(|| {
                *wr_counter -= 1;
                ArenaMutRef {_phantom: PhantomData, ptr}
            })
        }    
    }

    fn borrow<'a>(ptr: NonNull<Self>) -> Option<ArenaRef<'a, T>> {
        unsafe {
            let entry = ptr.as_ptr().as_mut().unwrap();
            let wr_counter = entry.wr_counter.get_mut();
            (*wr_counter >= 0).then(|| {
                *wr_counter += 1;
                ArenaRef {_phantom: PhantomData, ptr}
            })
        }
    }

    pub fn is_free(&self) -> bool {
        unsafe {
            *self.free.get().as_ref().unwrap()
        }
    }

    pub fn write(&mut self, data: T) -> Result<ArenaPeriod, T> {
        unsafe {
            if self.is_free() {
                self.data.get().as_mut().unwrap().write(data);
                let period = self.period.get().as_mut().unwrap();
                *self.free.get_mut() = false;
                *period += 1;
                Ok(*period)
            } else {
                Err(data)
            }
        }
    }
}

struct Bucket<T: Sized> {
    entries: NonNull<[Entry<T>]>,
    tail: Option<usize>,
    length: usize,
    layout: Layout
}
impl<T: Sized> Drop for Bucket<T> {
    fn drop(&mut self) {
        unsafe {
            for ptr in self.iter_entries_ptr() {
                ptr.drop_in_place();
            }

            dealloc(self.entries.cast::<u8>().as_ptr(), self.layout);
        }
    }
}
impl<T: Sized> Bucket<T> {
    fn new(size: usize) -> Self {
        unsafe {
            let layout = Layout::array::<Entry<T>>(size).unwrap();
            let raw = alloc(layout).cast::<MaybeUninit<Entry<T>>>();
            let slice_raw = std::ptr::slice_from_raw_parts_mut(raw, size);
            let mut maybe_uninnit_entries = NonNull::new(slice_raw).unwrap();
            
            for entry in maybe_uninnit_entries.as_mut().iter_mut() {
                entry.write(Entry::new());
            }

            let entries = std::mem::transmute(maybe_uninnit_entries);

            Self {entries, tail: None, layout, length: 0}
        }
    }

    unsafe fn iter_entries_ptr(&self) -> impl Iterator<Item=NonNull<Entry<T>>> + '_ {
        (0..self.len())
        .map(|entry| unsafe {
            self.get_entry_ptr(&entry)
        })
        .flatten()
    }

    fn alloc(&mut self, data: T) -> Result<(ArenaPeriod, ArenaEntryId), T> {
        if self.is_full() {
            return Err(data);
        }        

        let new_tail = self.tail.map(|tail| {
            tail + 1
        }).unwrap_or(0);

        unsafe {
            if let Some(mut entry) = self.get_entry_ptr_unchecked(&new_tail) {
                entry
                .as_mut()
                .write(data)
                .map(|period| (period, new_tail))
                .inspect(|_| {
                    self.tail = Some(new_tail);
                    self.length += 1;
                })
            } else {
                Err(data)
            }
        }
    }
    
    fn len(&self) -> usize {
        self.length    
    }

    unsafe fn get_entry_ptr(&self, entry: &ArenaEntryId) -> Option<NonNull<Entry<T>>> {
        self
        .get_entry_ptr_unchecked(entry)
        .filter(|ptr| ptr.as_ref().is_free() == false)
    }

    // Returns the entry by its id.
    unsafe fn get_entry_ptr_unchecked(&self, entry: &ArenaEntryId) -> Option<NonNull<Entry<T>>> {
        unsafe {
            self.entries
                .as_ptr().as_ref().unwrap()
                .get(*entry)
                .map(|entry_ref| std::ptr::from_ref(entry_ref) as *mut Entry<T>)
                .map(NonNull::new)
                .flatten()
        }
    }

    fn capacity(&self) -> usize {
        unsafe {
            self.entries.as_ref().len()
        }
    }

    fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }
}

struct BucketIter<'a, T>(std::slice::Iter<'a, Bucket<T>>);
impl<'a, T> Iterator for BucketIter<'a, T> {
    type Item = &'a Bucket<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

struct BucketEntryIter<'a, T>(std::slice::Iter<'a, Entry<T>>);
impl<'a, T> BucketEntryIter<'a, T> {
    pub fn new(bucket: &'a Bucket<T>) -> Self {
        unsafe {
            Self(bucket.entries.as_ref().iter())
        }
    }
}
impl<'a, T> Iterator for BucketEntryIter<'a, T> {
    type Item = &'a Entry<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

pub struct Iter<'a, T: Sized> {
    buckets: Enumerate<BucketIter<'a, T>>,
    bucket_id: usize,
    entries: Option<Enumerate<BucketEntryIter<'a, T>>>,
}


impl<'a, T: Sized> Iter<'a, T> {
    pub fn new(arena: &'a Arena<T>) -> Self {
        let buckets = arena.iter_buckets().enumerate();
        Self {
            buckets,
            entries: None,
            bucket_id: 0,
        }
    }

    pub fn next_bucket_entry(&mut self) -> Option<ArenaId> {
        if let Some(entries) = &mut self.entries {
            while let Some((entry_id, entry)) = entries.next() {
                if entry.is_free() == false {
                    return Some(ArenaId::new_with_period(
                        self.bucket_id, 
                        entry_id, 
                        unsafe {*entry.period.get().as_ref().unwrap()}
                    ))
                }
            }
            self.entries = None;
        }

        None
    }
}

impl<'a, T: Sized> Iterator for Iter<'a, T> {
    type Item = ArenaId;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.next_bucket_entry() {
            return Some(entry)
        }

        if let Some((bucket_id, bucket)) = self.buckets.next() {
            self.bucket_id = bucket_id;
            self.entries = Some(BucketEntryIter::new(bucket).enumerate());
        } else {
            return None
        }

        return self.next_bucket_entry()
    }
}

//
// An id-based arena dedicated to graph operations.
// The arena allows to borrow/mut borrow each entry independently. 
// 
// # Examples
// ```
// let mut arena = Arena::<u32>::new(30);
// let id_1 = arena.alloc(100);
// let entry = arena.borrow_mut(id_1);
// *entry = 20;
// let id_2 = arena.alloc(200);
// let entry_2 = arena.borrow_mut(id_2);
// *entry_2 = 30;
// ```
//
pub struct Arena<T: Sized> {
    buckets: Vec<Bucket<T>>,
    bucket_size: usize
}

impl<T: Sized> Arena<T> {
    pub fn new(block_size: usize) -> Self {
        Self {
            buckets: Vec::default(),
            bucket_size: block_size
        }
    }

    fn iter_buckets(&self) -> BucketIter<'_, T>{
        BucketIter(self.buckets.iter())
    }

    pub fn iter(&self) -> Iter<'_, T> {
        Iter::new(self)
    }

    pub fn borrow(&self, id: &ArenaId) -> Option<ArenaRef<'_, T>> {
        unsafe {
            self.get_entry_ptr(id)
            .map(Entry::borrow)
            .flatten()
        }
    }
    
    pub fn borrow_mut(&self, id: &ArenaId) -> Option<ArenaMutRef<'_, T>> {
        unsafe {
            self.get_entry_ptr(id)
            .map(Entry::borrow_mut)
            .flatten()
        }
    }

    pub fn alloc(&mut self, mut data: T) -> ArenaId {
        if let Some((bucket_id, bucket)) = self.find_free_bucket() {
            match bucket.alloc(data) {
                Ok((period, entry)) => return ArenaId::new_with_period(bucket_id, entry, period),
                Err(dat) => data = dat
            };
        }

        let mut bucket = Bucket::<T>::new(self.bucket_size);
        let (period, entry) = bucket.alloc(data).ok().unwrap();
        self.buckets.push(bucket);
        let block_id = self.buckets.len() - 1;

        return ArenaId::new_with_period(block_id, entry, period)
    }

    pub fn len(&self) -> usize {
        self.buckets
            .iter()
            .map(|block| block.len())
            .reduce(std::ops::Add::add)
            .unwrap_or_default()
    }
   
    unsafe fn get_entry_ptr(&self, id: &ArenaId) -> Option<NonNull<Entry<T>>> {
        self.buckets
        .get(id.bucket)
        .map(|block| block.get_entry_ptr_unchecked(&id.entry))
        .flatten()
    }
    
    fn find_free_bucket(&mut self) -> Option<(ArenaBucketId, &mut Bucket<T>)> {
        self.buckets
            .iter_mut()
            .enumerate()
            .find(|(_, block)| !block.is_full())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Arena, ArenaId};

    #[test]
    fn test_alloc() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let borrowed = arena.borrow(&id).unwrap();
        assert_eq!(*borrowed, 10);
    }

    #[test]
    fn test_must_create_another_block_if_previous_is_full() {
        let mut arena = Arena::<u32>::new(2);
        arena.alloc(1);
        arena.alloc(2);
        let id = arena.alloc(3);
        arena.alloc(4);

        assert_eq!(arena.len(), 4);

        let borrowed = arena.borrow(&id).unwrap();
        assert_eq!(*borrowed, 3);
    }

    #[test]
    fn test_iter() {
        let mut arena = Arena::<u32>::new(2);
        
        arena.alloc(1);
        arena.alloc(2);
        arena.alloc(3);
        arena.alloc(4);  

        let expected_ids = vec![
            ArenaId::new_with_period(0, 0, 1),
            ArenaId::new_with_period(0, 1, 1),
            ArenaId::new_with_period(1, 0, 1),
            ArenaId::new_with_period(1, 1, 1)
        ];

        let ids = arena.iter().collect::<Vec<_>>();
        assert_eq!(expected_ids, ids)
    }

    #[test]
    fn test_can_borrow_multiple_times() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let borrow_1 = arena.borrow(&id);
        let borrow_2 = arena.borrow(&id);

        assert!(borrow_1.is_some());
        assert!(borrow_2.is_some())
    }

    #[test]
    fn test_cannot_mut_borrow_if_already_borrowed() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let _ref = arena.borrow(&id).unwrap();
        assert_eq!(arena.borrow_mut(&id).is_none(), true); // cannot mut borrow multiple times.
    }

    #[test]
    fn test_cannot_mut_borrow_multiple_times() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let _mut_ref = arena.borrow_mut(&id).unwrap();
        assert_eq!(arena.borrow_mut(&id).is_none(), true); // cannot mut borrow multiple times.
    }

    #[test]
    fn test_can_mut_borrow_two_different_entries() {
        let mut arena = Arena::<u32>::new(2);
        let id_1 = arena.alloc(10);
        let id_2 = arena.alloc(20);

        let ref_1 = arena.borrow_mut(&id_1).unwrap();
        let ref_2 = arena.borrow_mut(&id_2).unwrap();

        assert_eq!(*ref_1, 10);
        assert_eq!(*ref_2, 20);
    }

    pub struct DropCanary<'a> {
        flag: Option<&'a mut bool>
    }

    impl<'a> Drop for DropCanary<'a> {
        fn drop(&mut self) {
            if let Some(f) = &mut self.flag {
                **f = true;
            }
        }
    }

    #[test]
    fn test_drop() {
        let mut flag = false;
        let mut arena = Arena::<DropCanary>::new(2);

        let id = arena.alloc(DropCanary { flag: Some(&mut flag)});

        assert_eq!(arena.borrow(&id).unwrap().flag, Some(&mut false));
        drop(arena);

        assert_eq!(flag, true);
    }
}