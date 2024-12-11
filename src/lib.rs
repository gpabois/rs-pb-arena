//! ID based arena for graph operations.

use std::{alloc::{alloc, dealloc, Layout}, cell::UnsafeCell, ops::{Deref, DerefMut}, ptr::NonNull};

pub mod thread;

pub struct ArenaRefMut<'a, T: Sized> {
    wr_counter: &'a UnsafeCell<i32>,
    data: &'a UnsafeCell<T> 
}

impl<T: Sized> Deref for ArenaRefMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.data.get().as_ref().unwrap()
        }
    }
}

impl<T: Sized> DerefMut for ArenaRefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe {
            self.data.get().as_mut().unwrap()
        }
    }
}


impl<T: Sized> Drop for ArenaRefMut<'_, T> {
    fn drop(&mut self) {
        unsafe {
            *self.wr_counter.get() += 1;
        }
    }
}


pub struct ArenaRef<'a, T: Sized> {
    wr_counter: &'a UnsafeCell<i32>,
    data: &'a UnsafeCell<T>
}

impl<T: Sized> Deref for ArenaRef<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe {
            self.data.get().as_ref().unwrap()
        }
    }
}

impl<T: Sized> Drop for ArenaRef<'_, T> {
    fn drop(&mut self) {
        unsafe {
            *self.wr_counter.get() -= 1;
        }
    }
}

struct ArenaCell<T: Sized> {
    wr_counter: UnsafeCell<i32>,
    data: UnsafeCell<T>
}

impl<T: Sized> ArenaCell<T> {
    pub fn new(data: T) -> Self {
        Self {
            wr_counter: UnsafeCell::new(0),
            data: UnsafeCell::new(data)
        }
    }
    

    pub fn borrow(&self) -> Option<ArenaRef<'_, T>> {
        unsafe {
            if *(self.wr_counter.get()) < 0 {
                return None
            }

            *self.wr_counter.get() += 1;
        }

        return Some(ArenaRef {
            wr_counter: &self.wr_counter,
            data: &self.data
        })
    }

    pub fn borrow_mut(&self) -> Option<ArenaRefMut<'_, T>> {
        unsafe {
            if *(self.wr_counter.get()) != 0 {
                return None
            }

            *self.wr_counter.get() -= 1;
        }

        Some(ArenaRefMut { wr_counter: &self.wr_counter, data: &self.data })
    }
}

struct ArenaBlock<T: Sized> {
    head: NonNull<ArenaCell<T>>,
    tail: NonNull<ArenaCell<T>>,
    last: Option<NonNull<ArenaCell<T>>>,
    layout: Layout
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ArenaCellId(usize);

impl From<usize> for ArenaCellId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct ArenaBlockId(usize);

impl From<usize> for ArenaBlockId {
    fn from(value: usize) -> Self {
        Self(value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd)]
pub struct ArenaId {
    block_id: ArenaBlockId,
    cell_id: ArenaCellId
}

impl Ord for ArenaId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.block_id != other.block_id {
            return self.block_id.cmp(&other.block_id);
        }

        return self.cell_id.cmp(&other.cell_id);
    }
}

impl ArenaId {
    fn new<ABI, ACI>(block_id: ABI, cell_id: ACI) -> Self 
    where ArenaBlockId: From<ABI>, ArenaCellId: From<ACI>
    {
        Self {block_id: block_id.into(), cell_id: cell_id.into()}
    }
    
    fn next_block(&self) -> Self {
        Self::new(self.block_id.0 + 1, 0)
    }
}

impl<T: Sized> Drop for ArenaBlock<T> {
    fn drop(&mut self) {
        unsafe {
            if let Some(last) = self.last {
                let mut cursor = self.head;
                
                while cursor <= last {
                    cursor.drop_in_place();
                    cursor = cursor.add(1);
                }

            }

            dealloc(self.head.cast::<u8>().as_ptr(), self.layout);
        }
    }
}

impl<T: Sized> ArenaBlock<T> {
    fn new(size: usize) -> Self {
        unsafe {
            let layout = Layout::array::<ArenaCell<T>>(size).unwrap();
            let head = NonNull::new(alloc(layout) as *mut ArenaCell<T>).unwrap();
            let tail = head.add(size - 1);
            let last = None;
            Self {head, tail, last, layout}
        }
    }

    fn first_id(&self) -> Option<ArenaCellId> {
        if self.len() == 0 {
            return None
        }

        return Some(ArenaCellId(0))
    }

    fn next_id(&self, id: ArenaCellId) -> Option<ArenaCellId> {
        self.last.map(|last| {
            unsafe {
                let cursor = self.head.add(id.0 + 1);
                (cursor <= last).then(|| ArenaCellId(id.0 + 1))
            }
        }).flatten()
    }

    fn alloc(&mut self, value: T) -> ArenaCellId {
        if self.is_full() {
            panic!("block is full")
        }        

        unsafe {
            let mut last = self.last
                .map(|last| last.add(1))
                .unwrap_or_else(|| self.head);

            *last.as_mut() = ArenaCell::new(value);
            self.last = Some(last);
            return ArenaCellId(self.len() - 1)
        }
    }

    fn len(&self) -> usize {
        self.last.map(|last| {
            unsafe {
                let offset: usize = last.offset_from(self.head).try_into().unwrap();
                offset + 1
            }     
        }).unwrap_or_default()       
    }

    // Returns the cell by its id.
    fn get_cell(&self, cell_id: ArenaCellId) -> Option<&ArenaCell<T>> {
        self.last
        .map(|last| {
            unsafe {
                let ptr = self.head.add(cell_id.0);
                
                (ptr <= last && ptr >= self.head)
                .then(|| ptr.as_ref())
            }
        }).flatten()
    }

    pub fn is_full(&self) -> bool {
        self.last
            .map(|last| last >= self.tail)
            .unwrap_or_else(|| false)
    }
}


pub struct ArenaIter<'a, T: Sized> {
    arena: &'a Arena<T>,
    id: Option<ArenaId>
}

impl<'a, T: Sized> ArenaIter<'a, T> {
    pub fn new(arena: &'a Arena<T>) -> Self {
        Self {
            arena,
            id: None
        }
    }
}

impl<'a, T: Sized> Iterator for ArenaIter<'a, T> {
    type Item = ArenaId;

    fn next(&mut self) -> Option<Self::Item> {
        if self.id.is_none() {
            self.id = self.arena.first_id();
            return self.id
        }

        let id = self.id.unwrap();
        self.id = self.arena.next_id(id);
        return self.id
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
    blocks: Vec<ArenaBlock<T>>,
    block_size: usize
}

impl<T: Sized> Arena<T> {
    pub fn new(block_size: usize) -> Self {
        Self {
            blocks: Vec::default(),
            block_size
        }
    }

    pub fn iter(&self) -> ArenaIter<'_, T> {
        ArenaIter::new(self)
    }

    pub fn borrow(&self, id: ArenaId) -> Option<ArenaRef<'_, T>> {
        self.get_cell(id)
        .map(ArenaCell::borrow)
        .flatten()
    }
    
    pub fn borrow_mut(&self, id: ArenaId) -> Option<ArenaRefMut<'_, T>> {
        self.get_cell(id).map(ArenaCell::borrow_mut).flatten()
    }

    pub fn alloc(&mut self, data: T) -> ArenaId {
        if let Some((block_id, block)) = self.find_free_block() {
            let cell_id: ArenaCellId = block.alloc(data);
            return ArenaId {block_id, cell_id}
        }

        let mut block = ArenaBlock::<T>::new(self.block_size);
        let cell_id = block.alloc(data);
        self.blocks.push(block);
        let block_id = ArenaBlockId(self.blocks.len() - 1);

        return ArenaId { block_id, cell_id }
    }

    pub fn len(&self) -> usize {
        self.blocks
            .iter()
            .map(|block| block.len())
            .reduce(std::ops::Add::add)
            .unwrap_or_default()
    }
   
    fn get_cell(&self, id: ArenaId) -> Option<&ArenaCell<T>> {
        self.blocks
        .get(id.block_id.0)
        .map(|block| block.get_cell(id.cell_id))
        .flatten()
    }
    
    fn find_free_block(&mut self) -> Option<(ArenaBlockId, &mut ArenaBlock<T>)> {
        self.blocks
            .iter_mut()
            .enumerate()
            .find(|(_, block)| !block.is_full())
            .map(|(block_id, block)| (ArenaBlockId(block_id), block))
    }

    fn first_id(&self) -> Option<ArenaId> {
        self.blocks
            .get(0)
            .map(|block| 
                block.first_id().map(|cell_id| ArenaId::new(0, cell_id))
            )
            .flatten()
    }

    fn next_block_id(&self, id: ArenaId) -> Option<ArenaId> {
        self.blocks
            .get(id.block_id.0 + 1)
            .map(|block| 
                block.first_id()
                    .map(|cell_id| ArenaId::new(
                        id.block_id.0 + 1,
                        cell_id
                    ))
            )
            .flatten()
    }

    fn next_id(&self, id: ArenaId) -> Option<ArenaId> {
        self.blocks
        .get(id.block_id.0)
        .map(|block| {
            block
            .next_id(id.cell_id)
            .map(|cell_id| ArenaId::new(id.block_id, cell_id))
        })
        .flatten()
        .or_else(|| self.next_block_id(id))
        
    }
}

#[cfg(test)]
mod tests {
    use crate::{Arena, ArenaId};

    #[test]
    fn test_alloc() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        assert_eq!(arena.len(), 1);
        assert!(arena.get_cell(ArenaId::new(0, 0)).is_some());

        let cell = arena.get_cell(id).unwrap();
        assert_eq!(*cell.borrow().unwrap(), 10);
    }

    #[test]
    fn test_must_create_another_block_if_previous_is_full() {
        let mut arena = Arena::<u32>::new(2);
        arena.alloc(1);
        arena.alloc(2);
        let id = arena.alloc(3);
        arena.alloc(4);

        assert_eq!(arena.len(), 4);

        let cell = arena.get_cell(id).unwrap();
        assert_eq!(*cell.borrow().unwrap(), 3);
    }

    #[test]
    fn test_iter() {
        let mut arena = Arena::<u32>::new(2);
        arena.alloc(1);
        arena.alloc(2);
        arena.alloc(3);
        arena.alloc(4);  

        assert_eq!(arena.next_block_id(ArenaId::new(0,1)), Some(ArenaId::new(1,0)));
        assert_eq!(arena.next_id(ArenaId::new(0,1)), Some(ArenaId::new(1,0)));

        let expected_ids = vec![
            ArenaId::new(0, 0),
            ArenaId::new(0, 1),
            ArenaId::new(1, 0),
            ArenaId::new(1, 1)
        ];

        let ids = arena.iter().collect::<Vec<_>>();
        assert_eq!(expected_ids, ids)
    }

    #[test]
    fn test_can_borrow_multiple_times() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let borrow_1 = arena.borrow(id);
        let borrow_2 = arena.borrow(id);

        assert!(borrow_1.is_some());
        assert!(borrow_2.is_some())
    }

    #[test]
    fn test_cannot_mut_borrow_if_already_borrowed() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let _ref = arena.borrow(id).unwrap();
        assert_eq!(arena.borrow_mut(id).is_none(), true); // cannot mut borrow multiple times.
    }

    #[test]
    fn test_cannot_mut_borrow_multiple_times() {
        let mut arena = Arena::<u32>::new(1);
        let id = arena.alloc(10);

        let _mut_ref = arena.borrow_mut(id).unwrap();
        assert_eq!(arena.borrow_mut(id).is_none(), true); // cannot mut borrow multiple times.
    }

    #[test]
    fn test_can_mut_borrow_two_different_entries() {
        let mut arena = Arena::<u32>::new(2);
        let id_1 = arena.alloc(10);
        let id_2 = arena.alloc(20);

        let ref_1 = arena.borrow_mut(id_1).unwrap();
        let ref_2 = arena.borrow_mut(id_2).unwrap();

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

        assert_eq!(arena.borrow(id).unwrap().flag, Some(&mut false));
        drop(arena);

        assert_eq!(flag, true);
    }
}