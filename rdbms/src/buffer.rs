use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::io;
use std::ops::{Index, IndexMut};
use std::rc::Rc;

use crate::disk::{DiskManager, PageId, PAGE_SIZE};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error("no free buffer available in buffer pool")]
    NoFreeBuffer,
}

#[derive(Debug, Default, Clone, Copy, Eq, PartialEq, Hash)]
pub struct BufferId(usize);

pub type Page = [u8; PAGE_SIZE];

// バッファ
#[derive(Debug)]
pub struct Buffer {
    pub page_id: PageId,
    pub page: RefCell<Page>,
    pub is_dirty: Cell<bool>,
}

impl Default for Buffer {
    fn default() -> Self {
        Self {
            page_id: Default::default(),
            page: RefCell::new([0u8; PAGE_SIZE]),
            is_dirty: Cell::new(false),
        }
    }
}

// フレーム
#[derive(Debug, Default)]
pub struct Frame {
    usage_count: u64,       // usage_count: バッファの利用回数
    buffer: Rc<Buffer>,
}

// バッファプール
pub struct BufferPool {
    buffers: Vec<Frame>,
    next_victim_id: BufferId,
}

impl BufferPool {

    pub fn new(pool_size: usize) -> Self {
        let mut buffers = vec![];
        buffers.resize_with(pool_size, Default::default);
        let next_victim_id = BufferId::default();
        Self {
            buffers,
            next_victim_id,
        }
    }

    pub fn size(&self) -> usize {
        self.buffers.len()
    }
    // Clock-sweep(どのバッファを捨てるか決めるアルゴリズム)
    // PostgreSQLでも採用されているアルゴリズム
    fn evict(&mut self) -> Option<BufferId> {
        let pool_size = self.size();
        let mut consecutive_pinned = 0;

        let victim_id = loop {
            let next_victim_id = self.next_victim_id;
            let frame = &mut self[next_victim_id];
            if frame.usage_count == 0 {
                break self.next_victim_id;
            }
            // バッファが貸出中かどうか
            if Rc::get_mut(&mut frame.buffer).is_some() {
                // 貸出中でなければデクリメント
                frame.usage_count -= 1;
                consecutive_pinned = 0;
            } else {
                // 貸出中
                consecutive_pinned += 1;
                if consecutive_pinned >= pool_size {
                    return None;
                }
            }
            self.next_victim_id = self.increment_id(self.next_victim_id);
        };
        Some(victim_id)
    }

    fn increment_id(&self, buffer_id: BufferId) -> BufferId {
        BufferId((buffer_id.0 + 1) % self.size())
    }
}

impl Index<BufferId> for BufferPool {
    type Output = Frame;

    fn index(&self, index: BufferId) -> &Self::Output {
        &self.buffers[index.0]
    }
}

impl IndexMut<BufferId> for BufferPool {
    fn index_mut(&mut self, index: BufferId) -> &mut Self::Output {
        &mut self.buffers[index.0]
    }
}

// バッファプールマネージャ
pub struct BufferPoolManager {
    // バッファプール内に必要なページのキャッシュがない場合、ディスクマネージャを呼び出して
    // ヒープファイルからデータを読み込む
    disk: DiskManager,
    pool: BufferPool,
    page_table: HashMap<PageId, BufferId>,      // ページテーブル: ページIDとバッファIDの対応表
}

impl BufferPoolManager {
    pub fn new(disk: DiskManager, pool: BufferPool) -> Self {
        let page_table = HashMap::new();
        Self {
            disk,
            pool,
            page_table,
        }
    }

    // ページの貸出
    fn fetch_page(&mut self, page_id: PageId) -> Result<Rc<Buffer>, Error> {
        // ページがバッファプールにある場合
        if let Some(&buffer_id) = self.page_table.get(&page_id) {
            let frame = &mut self.pool[buffer_id];
            frame.usage_count += 1;
            return Ok(Rc::clone(&frame.buffer));
        }

        // ページがバッファプールにない場合
        
        // 1.捨てるバッファ = 次に読み込むページを格納するバッファを決定
        let buffer_id = self.pool.evict().ok_or(Error::NoFreeBuffer)?;
        let frame = &mut self.pool[buffer_id];
        let evict_page_id = frame.buffer.page_id;
        {
            let buffer = Rc::get_mut(&mut frame.buffer).unwrap();

            // 2.捨てるバッファのis_dirtyフラグがtrueなら、そのバッファをディスクに書き出す。
            if buffer.is_dirty.get() {
                self.disk.write_page_data(page_id, buffer.page.get_mut())?;
            }
            buffer.page_id = page_id;
            buffer.is_dirty.set(false);

            // 3.ページを読み出し
            self.disk.read_page_data(page_id, buffer.page.get_mut())?;
            frame.usage_count = 1;
        }
        let page = Rc::clone(&frame.buffer);
        
        // 4.バッファに入っているページが入れ替わったので、ページテーブルを更新する
        self.page_table.remove(&evict_page_id);
        self.page_table.insert(page_id, buffer_id);
        Ok(page)
    }
}