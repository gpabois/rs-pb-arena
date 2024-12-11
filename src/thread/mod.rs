use std::{alloc::{alloc, dealloc, Layout}, marker::PhantomData, ops::{Deref, DerefMut}, ptr::NonNull, sync::atomic::{AtomicI32, AtomicIsize}};

use pb_atomic_linked_list::AtomicLinkedList;

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

/// A thread-safe arena allocator.
pub struct Arena<T>{
    blocks: AtomicLinkedList<ArenaBlock<T>>,
    block_size: usize
}

impl<T> Arena<T> {
    pub fn alloc(&mut self, data: T) -> ArenaId {
        // Find a suitable block with remaining space.
        // Optimisation can be done.
        for (block_id, block) in self.blocks.iter().enumerate() {
            unsafe {
                if let Some((cell_id, mut uninit_cell)) = block.raw_alloc() {
                    *uninit_cell.as_mut() = ArenaCell::new(data);
                    return ArenaId {
                        block_id: ArenaBlockId(block_id),
                        cell_id
                    }
                }
            }
        }

        // We need to create a new block
        let block = ArenaBlock::<T>::new(self.block_size);
        let cell_id = block.alloc(data).unwrap();
        let block_id = ArenaBlockId(self.blocks.insert(block));

        return ArenaId { block_id, cell_id }
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
        self.blocks.borrow(block_id.0)
    }
}