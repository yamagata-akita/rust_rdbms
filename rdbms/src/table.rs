use anyhow::Result;

use crate::btree::BTree;
use crate::buffer::BufferPoolManager;
use crate::disk::PageId;
use crate::tuple;

#[derive(Debug)]
pub struct SimpleTable {
    pub meta_page_id: PageId,   // テーブルの内容が入っているB+TreeのメタページのID
    pub num_key_elems: usize,   // 左からいくつの列がプライマリキーなのかを示す
}

impl SimpleTable {
    pub fn create(&mut self, bufmgr: &mut BufferPoolManager) -> Result<()> {
        let btree = BTree::create(bufmgr)?;
        self.meta_page_id = btree.meta_page_id;
        Ok(())
    }

    pub fn insert(&self, bufmgr: &mut BufferPoolManager, record: &[&[u8]]) -> Result<()> {
        let btree = BTree::new(self.meta_page_id);
        // プライマリキーの部分 : record[..self.num_key_elems]
        // それ以外             : record[self.num_key_elems..]
        let mut key = vec![];
        tuple::encode(record[..self.num_key_elems].iter(), &mut key);   // encodeはtuple::encodeを使っている
        let mut value = vec![];
        tuple::encode(record[self.num_key_elems..].iter(), &mut value);
        btree.insert(bufmgr, &key, &value)?;
        Ok(())
    }
}