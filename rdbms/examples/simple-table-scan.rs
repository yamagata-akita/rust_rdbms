use anyhow::Result;

use rdbms::btree::{BTree, SearchMode};
use rdbms::buffer::{BufferPool, BufferPoolManager};
use rdbms::disk::{DiskManager, PageId};
use rdbms::tuple;

fn main() -> Result<()> {
    let disk = DiskManager::open("simple.rly")?;
    let pool = BufferPool::new(10);
    let mut bufmgr = BufferPoolManager::new(disk, pool);

    let btree = BTree::new(PageId(0));

    // プライマリキー以外での検索では、フルスキャンを行う
    let mut iter = btree.search(&mut bufmgr, SearchMode::Start)?;

    while let Some((key, value)) = iter.next(&mut bufmgr)? {
        let mut record = vec![];
        tuple::decode(&key, &mut record);
        tuple::decode(&value, &mut record);
        if record[2] == b"Smith" {
            println!("{:?}", tuple::Pretty(&record));
        }
    }
    Ok(())
}