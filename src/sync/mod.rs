use std::{alloc::{alloc, dealloc, Layout}, cell::UnsafeCell, marker::PhantomData, ops::{Deref, DerefMut}, ptr::NonNull, sync::{atomic::{AtomicI32, AtomicIsize}, Arc}};

use pb_atomic_linked_list::{prelude::AtomicLinkedList as _, AtomicLinkedList, AtomicQueue};

use crate::{ArenaBlockId, ArenaCellId, ArenaId};

pub struct ArenaRef<'a, T> {
    _phantom: PhantomData<&'a ()>,
    ptr: NonNull<ArenaCell<T>>
}

impl<'a, T> Deref for ArenaRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {&self.ptr.as_ref().data}
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
    ptr: NonNull<ArenaCell<T>>
}

impl<'a, T> Deref for ArenaMutRef<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {&self.ptr.as_ref().data}
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

struct ArenaCell<T> {
    wr_lock: AtomicI32,
    data: T
}

impl<T> ArenaCell<T> {
    fn new(data: T) -> Self {
        Self {
            wr_lock: AtomicI32::new(0),
            data
        }
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

struct ArenaBlock<T> {
    head: NonNull<ArenaCell<T>>,
    tail: NonNull<ArenaCell<T>>,
    last: AtomicIsize,
    layout: Layout
}

impl<T: Sized> Drop for ArenaBlock<T> {
    fn drop(&mut self) {
        unsafe {
            let last = self.last.load(std::sync::atomic::Ordering::Relaxed);
            if last >= 0 {
                let mut cursor = self.head;
                
                while cursor.offset_from(self.head) <= last {
                    cursor.drop_in_place();
                    cursor = cursor.add(1);
                }

            }

            dealloc(self.head.cast::<u8>().as_ptr(), self.layout);
        }
    }
}

impl<T> ArenaBlock<T> {
    fn new(size: usize) -> Self {
        unsafe {
            let layout = Layout::array::<ArenaCell<T>>(size).unwrap();
            let head = NonNull::new(alloc(layout) as *mut ArenaCell<T>).unwrap();
            let tail = head.add(size - 1);
            let last = AtomicIsize::new(-1);
            Self {head, tail, last, layout}
        }
    }

    fn get_cell(&self, cell_id: &ArenaCellId) -> Option<NonNull<ArenaCell<T>>> {
        unsafe {
            let ptr = self.head.add(cell_id.0);
        
            if self.len() <= cell_id.0 {
                return None
            }
    
            Some(ptr)
        }
    }
    fn len(&self) -> usize {
        let offset = self.last.load(std::sync::atomic::Ordering::Relaxed);
        if offset < 0 { return 0 } 
        return offset as usize; 
    }
    
    
    fn capacity(&self) -> usize {
        unsafe {
            self.tail.offset_from(self.head).try_into().unwrap()
        }
    }

    fn is_full(&self) -> bool {
        self.len() >= self.capacity()
    }

    fn alloc(&self, data: T) -> Option<ArenaCellId> {
        unsafe {
            self.raw_alloc().map(|(cell_id, mut cell)| {
                *cell.as_mut() = ArenaCell::new(data);
                cell_id
            })
        }
    }

    /// Raw allocate space for an arena cell.
    unsafe fn raw_alloc(&self) -> Option<(ArenaCellId, NonNull<ArenaCell<T>>)> {
        if self.is_full() {
            return None
        }

        let offset = self
            .last.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1isize;
        
        unsafe {
            let ptr = self.head.add(offset as usize);
            // overflowed...
            if ptr > self.tail {
                self.last.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                return None;
            }

            return Some((ArenaCellId(offset as usize), ptr))
            
        }     
    }

}

struct InnerArena<T> {
    blocks: UnsafeCell<AtomicLinkedList<ArenaBlock<T>>>,
    block_size: usize,
    free_blocks: UnsafeCell<AtomicQueue<ArenaBlockId>>,
}

unsafe impl<T> Sync for InnerArena<T> {}
unsafe impl<T> Send for InnerArena<T> {}


/// A thread-safe arena allocator.
pub struct Arena<T>(Arc<InnerArena<T>>);

impl<T> Clone for Arena<T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<T> Arena<T> {
    pub fn new(block_size: usize) -> Self {
        let inner = InnerArena {
            blocks: UnsafeCell::new(AtomicLinkedList::new()),
            block_size,
            free_blocks: UnsafeCell::new(AtomicQueue::new())
        };

        Self(Arc::new(inner))
    }

    pub fn alloc(&mut self, data: T) -> ArenaId {
        unsafe {
            // We check the free blocks queue
            if let Some(block_id) = self.0.free_blocks.get().as_mut().unwrap().dequeue() {
                if let Some(block) = self.get_block(&block_id) {
                    if let Some((cell_id, mut cell)) = block.raw_alloc() {
                        if !block.is_full() {
                            self.0.free_blocks.get().as_mut().unwrap().enqueue(block_id);
                        }
                        
                        *cell.as_mut() = ArenaCell::new(data);
                        return ArenaId {block_id, cell_id};
                    }
                }      
            }

            // We need to create a new block
            let block = ArenaBlock::<T>::new(self.0.block_size);
            let cell_id = block.alloc(data).unwrap();
            let block_id = ArenaBlockId(self.0.blocks.get().as_mut().unwrap().insert(block));

            // Add it to the free block register
            self.0.free_blocks.get().as_mut().unwrap().enqueue(block_id);

            return ArenaId { block_id, cell_id }
        }

    }

    pub fn borrow<'a>(&'a self, id: &ArenaId) -> Option<ArenaRef<'a, T>> {
        self.get_cell(id).and_then(|cell| ArenaCell::borrow::<'a>(cell))
    }

    pub fn borrow_mut<'a>(&'a self, id: &ArenaId) -> Option<ArenaMutRef<'a, T>> {
        self.get_cell(id).and_then(|cell| ArenaCell::borrow_mut::<'a>(cell))       
    }

    fn get_cell(&self, id: &ArenaId) -> Option<NonNull<ArenaCell<T>>> {
        self.get_block(&id.block_id).and_then(|block| block.get_cell(&id.cell_id))
    }

    fn get_block(&self, block_id: &ArenaBlockId) -> Option<&ArenaBlock<T>> {
        unsafe {
            self.0.blocks.get().as_ref().unwrap().borrow(block_id.0)
        }
    }
}

#[cfg(test)]
mod test {
    use std::thread;

    use super::Arena;

    #[test]
    fn test_can_alloc_in_multiple_threads() {
        let arena = Arena::<u32>::new(10);
        
        let mut arena_1 = arena.clone();
        let mut arena_2 = arena.clone();

        let j1 = thread::spawn(move || {
            for i in 0..=10_000 {
                arena_1.alloc(i);
            }
        });

        let j2 = thread::spawn(move || {
            for i in 0..=20_000 {
                arena_2.alloc(i);
            }
        });

        j1.join().unwrap();
        j2.join().unwrap();


    }
}